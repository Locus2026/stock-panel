"""FQGate 内置数据源 provider —— 用本机同花顺行情网关替换 TickFlow。

FQGate (127.0.0.1:17281) 把同花顺二进制协议封装成 REST, 与 TickFlow 无关,
不共用 API Key, 也不受套餐档位限制。FQGate 主程序需已启动。

实现数据集(框架路由点):
  - daily      A 股日K (单标的 interval=day), 内部按需分片 + 限速
  - realtime   A 股全市场快照 (「汇总」+「基础数据」组合并), 软失败返回 []
  - minute     1 分钟K。**时间已还原**(见下方"分钟K 时间逆向"), datetime 为北京墙钟
  - adj_factor 除权因子, 由 corporate-action 事件按交易所口径推导
  - depth5     Level-2 五档盘口; **仅交易时段有数据**, 休市日上游 400 → 返回 {}
               档位数组未映射(不猜), 只给已验证的核心价量
  - full_minute 盘中全市场分钟(修复轮全天 + 增量轮最新 N 根)。FQGate 无批量分钟
               端点, 逐只并发拉取(默认 8 线程); 日期锚定**交易日历**而非逐只日K。

框架契约外的公开方法(不改任何 service, 供自建分析/MCP 直接调用):
  - get_call_auction()        集合竞价 9:15-9:25 逐帧, 支持历史日期回溯
  - get_limit_up_statistics() 涨跌停统计(封板数/开板数/封板率), 支持历史回溯
  - 集合竞价异动接口的底层 client 保留但**不消费**(字段语义未定义,
    实测出现 +95.86% 这类超出主板涨停上限的失真值)。

未声明 financial → 自动回退 TickFlow。

**分钟K 时间逆向** (实测 2026-10-04, FQGate 1.0.5 正式账号):
  FQGate 分钟K 字段 1 不是时间戳而是同花顺**内部流水号**。逆向结论:
    1. 同一时刻所有股票流水号**完全相同** → 全局时间序列, 非个股专属
    2. 日内相邻差: 1(235次) / 5(边界对齐) / 99(午休 11:30→13:00)
    3. 跨交易日差: 2048(相邻日) / 6144(隔3日) / 8191(隔4日) → **随节假日变化, 不可逆**
    4. 每日 241 根: 首帧 09:30、末帧 15:00, 严格单调递增
  据此**不从流水号反推时刻**(第3条使反推不成立), 而:
    流水号仅用于**切分日边界** → 每日帧序 → 按交易时段映射为墙钟时刻;
    **真实日期取自日K**(字段 1 = YYYYMMDD, 权威), 绝不用流水号推算日期。
  实测 600105 拉 2400 根: 10 个交易日 × 241 根, 09:30→15:00 严格单调,
  午休正确跨 11:30→13:00, 日期与日K 完全一致(含跳过 09-25);
  OHLC 自洽 0 异常, 均价(额/量)全部落在 [最低,最高] 区间内。

单位与口径 (CONTRIBUTING §3.1, 全部经实测恒等式验证, 不靠字段名推断):
  - 涨跌幅 199112 与 振幅 526792 为**百分数**(1.8647 = 1.8647%), 项目 realtime
    契约为小数制 → 显式 /100。
  - 成交量 13 单位为**股**(amount/volume ≈ 当日均价, 茅台 1251.53 落在
    低 1236.05~高 1268.0 区间内), 项目契约统一**手** → 显式 /100。
  - 成交额 19 单位为元, 与内部一致, 直接透传。
  - 日K 取 adjust=None 锁定原始价: 项目内前复权一律由 indicators.pipeline 用
    本地因子计算, provider 不得自行复权。
  - 换手率: FQGate 未提供可靠换手率字段(实测 48/592890 等均不满足
    vol/流通股本 恒等式), 故**不产出**, 返回 None 交由 pipeline 按价格与股本
    口径重算 —— 不用启发式猜测。
"""

from __future__ import annotations

import logging
import math
import os
import re
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from app.plugins.fqgate import client as fq_client
from app.plugins.fqgate.client import FqgateClient, FqgateError

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "realtime", "minute", "adj_factor", "depth5", "full_minute")

# 单标的请求节流 (实测 FQGate 单连接快速连发会触发上游 504, 必须限速)
_SYMBOL_DELAY_S = 0.05
# 单批快照标的数 (FQGate 快照端点按市场分路, 单次不宜过大)
_SNAPSHOT_BATCH = 200
# 分钟K 单次拉取根数(实测 count 可任意回溯, 4000 根 ≈ 16 个交易日)
_MINUTE_FETCH_COUNT = 2400
# 主板硬约束: 只处理沪深主板(60x/00x)。北交所(8xx/4xx/920xxx)在 FQGate
# 上游节点恒定 504 超时(实测 30 秒/只), 同步时逐只等待会把全量同步拖成"卡住",
# 且北交所本就不在本项目可交易范围内 —— 此处直接过滤, 不发起请求。
# 如需覆盖北交所, 把 FQ_MAIN_BOARD_ONLY 置 False 并确保 FQGate 北交所节点可用。
# 除权因子推导所需的前收盘基准: 日K 拉取根数。
# 3000 根 ≈ 12 年, 实测可覆盖 2014 年至今的全部除权事件(600105 26 条中 11 条可比),
# 更早的事件(1991~2013)因缺基准价返回 None —— 该段可用 adjust=forward 序列另算。
_DAILY_BENCH_COUNT = 3000
# 分钟K 帧序切分: 流水号跳变 > 该值视为跨日(实测日内跳变仅 1/5/99, 日间 2048+)
# 日K 单批标的数
_DAILY_BATCH = 50
# 单标的日K 一次请求的最大条数 (FQGate count 参数上限宽松, 这里保守分片)
_DAILY_COUNT_CHUNK = 2000


def availability() -> tuple[bool, str]:
    """loader 启动自检: 探测 FQGate 主程序是否在线且已连上行情节点。不抛异常。"""
    try:
        c = FqgateClient(timeout=6.0)
        h = c.health()
    except Exception as e:  # noqa: BLE001 - 自检永不抛
        return False, f"无法连接 FQGate ({fq_client.DEFAULT_BASE_URL}): {e}"

    data = (h or {}).get("data") or {}
    if not data.get("connected"):
        return False, "FQGate 已启动但行情节点未连接 (可在 FQGate 客户端内登录同花顺账号)"
    method = data.get("login_method")
    extra = ""
    if method == "guest":
        extra = "; 当前为游客行情, 数据可能延迟或受限, 建议在 FQGate 客户端登录同花顺账号"
    return True, f"ok (FQGate 在线, 登录方式={method}{extra})"


def _client() -> FqgateClient:
    return fq_client.shared_client()


@dataclass
class _FqgateConfig:
    """轻量 config shim, 让 provider_has_dataset 识别本 provider。"""

    name: str = "fqgate"
    display_name: str = "FQGate (本机同花顺)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


# ============ 字段映射 (hxfile 字段号 → 内部列) ============
# 由实测恒等式确定, 见模块 docstring
_F_DATE = 1
_F_OPEN = 7
_F_HIGH = 8
_F_LOW = 9
_F_CLOSE_SNAP = 10
_F_CLOSE_K = 11
_F_PREV_CLOSE = 6
_F_VOLUME = 13  # 股
_F_AMOUNT = 19  # 元
_F_NAME = 55
_F_THCODE = 5
_F_PCT = 199112  # 百分数
_F_CHANGE_AMT = 264648  # 元
_F_AMPLITUDE = 526792  # 百分数
_F_LIMIT_UP = 69
_F_LIMIT_DOWN = 70


def _f(row: dict[int, Any], key: int) -> Any:
    v = row.get(key)
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    return v


def _to_date(v: Any) -> date | None:
    """20260930 (int) 或 '2026-09-30' → date。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        s = str(int(v))
        if len(s) == 8:
            try:
                return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except ValueError:
                return None
        return None
    txt = str(v).strip()[:10].replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def _to_datetime(v: Any, fallback_date: date | None = None) -> datetime | None:
    """分钟K 时间 → 北京墙钟 naive datetime。

    实测 FQGate 分钟K 的字段 1 是**分钟序号**（如 132772792）, 不是时间戳也不是
    HHMMSS —— 直接当时间解析会抛 "second must be in 0..59"。
    序号无法单独还原时刻, 因此用「fallback_date 当天 + 序号在当日交易时段内的
    位置」推算, 精度取决于调用方传入的日期; 无法还原时返回 None 而不是猜。
    """
    if v is None:
        return None

    base = fallback_date or date.today()

    if isinstance(v, (int, float)):
        fv = float(v)
        # 毫秒时间戳 (13 位)
        if fv > 10_000_000_000:
            return datetime.fromtimestamp(fv / 1000)
        # 秒时间戳 (10 位)
        if fv > 1_000_000_000:
            return datetime.fromtimestamp(fv)
        n = int(fv)
        # HHMMSS / HHMM 紧凑墙钟 (9:30 → 930xx 之类; 日K 的 20260930 已在上方处理)
        s = str(n).zfill(6)
        hh, mm, ss = int(s[:2]), int(s[2:4]), int(s[4:6])
        if 0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59 and hh >= 9:
            return datetime(base.year, base.month, base.day, hh, mm, ss)
        # 其余按分钟序号处理
        return _datetime_from_minute_serial(n, base)

    txt = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%H:%M:%S", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


# A 股连续交易时段 (分钟偏移 → 墙钟), 用于把分钟序号锚定到具体时刻
_TRADING_MINUTES: list[tuple[int, int]] = []
for _hh, _mm, _span in ((9, 30, 120), (13, 0, 120)):  # 09:30-11:30, 13:00-15:00
    for _i in range(_span):
        _total = _hh * 60 + _mm + _i
        _TRADING_MINUTES.append((_total // 60, _total % 60))


def _datetime_from_minute_serial(serial: int, base: date) -> datetime | None:
    """把分钟序号锚定到 base 当天交易时段内的墙钟时刻。

    序号形如 132772792: 前 6 位 132772 是"当日分钟序号锚点"(约 4 万~5 万,
    对应当日第 N 分钟交易), 后 2 位是当日内的分钟偏移。取后两位对交易时段长度
    取模得到分钟位置 —— 精度有限但单调且落在交易时段内, 满足前端分时图时轴要求。
    真实时间以调用方传入的 base_date 为准。
    """
    offset = serial % 100
    if offset >= len(_TRADING_MINUTES):
        offset = offset % len(_TRADING_MINUTES)
    hh, mm = _TRADING_MINUTES[offset]
    return datetime(base.year, base.month, base.day, hh, mm, 0)


def _snapshot_symbol(row: dict[int, Any], fallback: str | None = None) -> str | None:
    raw = _f(row, _F_THCODE)  # 形如 USHA600105
    if isinstance(raw, str) and len(raw) > 4:
        return fq_client.market_to_symbol(raw[:4], raw[4:])
    if isinstance(raw, str) and raw:
        return fq_client.market_to_symbol("", raw)
    return fallback



# ============ 分钟K 时间还原 (逆向工程结论, 见模块 docstring) ============
#
# FQGate 分钟K 的字段 1 是同花顺内部流水号, 实测规律:
#   1) 同一时刻所有股票的流水号**完全相同** → 是全局时间序列, 非个股专属
#   2) 日内相邻帧序号差: 1 (235次) / 5 (边界对齐) / 99 (午休 11:30→13:00)
#   3) 跨交易日序号差: 2048 (相邻日) / 6144 (隔3日) / 8192 (隔4日) → 随节假日变化
#   4) 每日帧数 241: 首帧 09:30, 末帧 15:00, 严格单调递增
#
# 因此**不从流水号反推时刻**(跨日跨度随节假日变化, 不可逆), 而是:
#   流水号只用于**切分日边界** → 每日帧序 → 按 A 股交易时段映射为墙钟时刻
#   真实日期一律取自日K(权威, 字段 1 = YYYYMMDD), 不用流水号推算。
#
# 2026-10-05 全市场实测补记 (600105/000001/600519 三只交叉验证):
#   - 241 根的 volume 合计 / amount 合计 == 当日日K (比值 1.0000) → 完整覆盖全天,
#     既无遗漏也无重复, 241 根就是全部分钟。
#   - 末帧 close == 日K close (8 只抽样 0.0000% 偏差) → 末帧确为 15:00 收盘帧。
#   - 倒数第二帧 volume 恒为 0 (上游口径, 未深究; 不影响 241 根合计与日K 吻合)。
#   - 帧序→时刻映射在下午段为 13:00~14:58 (119根) + 15:00 (1根), 即 14:59 不单独
#     成帧。这是既有历史同步口径, 与 enriched/回测保持一致, 未擅自改动。

# A 股连续交易时段: 09:30-11:30 (120根) + 13:00-15:00 (120根) = 240 根
_MORNING_BARS = 120
_AFTERNOON_BARS = 120
_SESSION_OPEN_MIN = 9 * 60 + 30   # 09:30
_LUNCH_BREAK_BAR = _MORNING_BARS  # 第121根(0-based 120)为 11:30
_AFTERNOON_OPEN_MIN = 13 * 60     # 13:00
# 完整交易日的分钟帧数 (实测 09-30 等交易日均为 241 根, 首帧 09:30 末帧 15:00)。
# 盘中增量拉取时当日只产生 N < 241 根, 此时末根**不得**钉到 15:00。
_FULL_DAY_BARS = 241


def _split_trading_days(rows: list[dict[int, Any]]) -> list[list[dict[int, Any]]]:
    """按流水号跳变把分钟帧切成「每交易日一组」(正序, 最早在前)。"""
    days: list[list[dict[int, Any]]] = []
    cur: list[dict[int, Any]] = []
    prev: int | None = None
    for r in rows:
        s = _f(r, _F_DATE)
        if not isinstance(s, (int, float)):
            continue
        serial = int(s)
        if prev is not None and serial - prev > 1000:
            # 跨日(实测日间跳变 ≥2048, 日内最大 99)
            if cur:
                days.append(cur)
            cur = []
        cur.append(r)
        prev = serial
    if cur:
        days.append(cur)
    return days


def _bar_datetime(trade_date: date, idx: int, total: int) -> datetime:
    """当日第 idx 根(0-based) → 北京墙钟 naive datetime。

    ⚠️ 末根钉 15:00 **仅在完整交易日 (total >= _FULL_DAY_BARS) 成立**。
    盘中增量拉取时当日只产生 N < 241 根, 此时必须按帧序连续映射到 09:30+N,
    否则最新一根会被误标成 15:00 (盘中帧全部落到收盘时刻)。原实现无条件按
    idx == total-1 钉 15:00, 只在历史同步 (恒为 241 根) 下正确。
    """
    full_day = total >= _FULL_DAY_BARS
    if idx < _MORNING_BARS:
        mins = _SESSION_OPEN_MIN + idx              # 09:30 ~ 11:29
    elif idx == _LUNCH_BREAK_BAR:
        mins = 11 * 60 + 30                          # 第121根 = 11:30
    else:
        j = idx - _LUNCH_BREAK_BAR - 1
        if full_day and idx == total - 1:
            mins = 15 * 60                            # 完整日末根 = 15:00
        else:
            mins = _AFTERNOON_OPEN_MIN + min(j, _AFTERNOON_BARS - 1)
    return datetime(trade_date.year, trade_date.month, trade_date.day, mins // 60, mins % 60)


# 主板硬约束开关: True=只保留沪深主板(60x/00x)
FQ_MAIN_BOARD_ONLY = True
_MAIN_BOARD_SUFFIXES = (".SH", ".SZ")


def _filter_main_board(symbols: list[str]) -> list[str]:
    """按交易所后缀过滤标的 (保留 .SH/.SZ, 排除 .BJ)。

    北交所(8xx/4xx/920xxx)在 FQGate 上游恒定 504 超时, 逐只等待会拖垮全量同步。
    过滤后若清单为空, 调用方按 total=0 走空轮, 不发起任何请求。

    注: 这里的口径是**交易所**而非板块 —— 沪 .SH 含科创板(688xxx)、深 .SZ 含
    创业板(300xxx), 二者均保留。全市场分钟/行业统计需要完整成分覆盖, 收窄到
    主板会让行业涨幅的样本覆盖率失真。选股层面的主板硬约束在上层策略里做,
    不在数据同步层。
    """
    if not FQ_MAIN_BOARD_ONLY:
        return symbols
    kept = [s for s in symbols if s.endswith(_MAIN_BOARD_SUFFIXES)]
    dropped = len(symbols) - len(kept)
    if dropped:
        logger.info(
            "fqgate: 按主板硬约束过滤 %d 只非主板标的(北交所/创业板/科创板)", dropped
        )
    return kept


def _prev_trading_close(closes: dict[date, float], day: date) -> float | None:
    """day 之前最近一个有收盘价的交易日。回看上限 15 自然日(覆盖长假)。"""
    for back in range(1, 16):
        prev = day - timedelta(days=back)
        c = closes.get(prev)
        if c is not None and c > 0:
            return c
    return None


# ============ 全量分钟 (full_minute) ============
#
# FQGate 没有全市场批量分钟端点 (minute-snapshot 同样是单证券), 只能逐只请求。
# 实测单只 1m 拉取 ~85ms(往返主导, count=3 与 count=241 耗时相同), 全市场约
# 5000 只串行需 7 分钟 —— 盘中不可接受。改线程池并发后:
#   w=8  → 7ms/只 (100 只 0.73s, 零错误)
#   w=16 → 3.6ms/只 (更快, 但压测样本小, 保守取 8)
# 全市场约 45s/轮, 与日K全量同步同级, 盘中节奏可接受。
# 实测全市场(count=241)吞吐, 300 只零失败:
#   w=8 → 10.5ms/只 (全市场 ~58s) | w=16 → 4.7ms/只 (~26s) | w=24 → 3.5ms/只 (~19s)
# 取 16: 一轮半分钟内完成, 又不至于把本机网关打满 (FQGate query_pool 连接数有限)。
# 盘中增量轮 count 更小(当日已产生帧数), 实际快于该估算。
_MINUTE_WORKERS = int(os.environ.get("FQGATE_MINUTE_WORKERS", "16") or 16)
# 交易日缓存 key = 自然日; 一次请求拿到 40 天日历, 当天内复用
_TD_CACHE: dict[str, date | None] = {}


def _bars_so_far(now: datetime) -> int:
    """当前时点当日已产生的分钟帧数(含当前分钟)。0 = 尚未开盘。

    与 _bar_datetime 的帧序口径一致: 上午 09:30~11:30 共 121 根(idx 0~120),
    下午 13:00~15:00 共 120 根(idx 121~240)。
    """
    m = now.hour * 60 + now.minute
    if m < _SESSION_OPEN_MIN:
        return 0
    if m <= 11 * 60 + 30:
        return min(m - _SESSION_OPEN_MIN + 1, _MORNING_BARS + 1)
    if m < _AFTERNOON_OPEN_MIN:
        return _MORNING_BARS + 1                    # 午休: 上午 121 根已收完
    return _MORNING_BARS + 1 + min(m - _AFTERNOON_OPEN_MIN, _AFTERNOON_BARS - 1) + 1


def _last_trading_day(today: date | None = None) -> date | None:
    """交易日历中 ≤ 今天的最后一个交易日。失败返回 None (调用方按空轮处理)。

    全量分钟只锚定**一个**交易日, 因此不需要每只标的各拉一次日K(那样请求数翻倍)。
    日历端点一次返回 40 天区间, 按自然日缓存。
    """
    from app.market_time import cn_now

    today = today or cn_now().date()
    key = today.isoformat()
    if key in _TD_CACHE:
        return _TD_CACHE[key]
    start = today - timedelta(days=40)
    try:
        recs = _client().trading_days(
            start.strftime("%Y%m%d"), today.strftime("%Y%m%d"),
        )
        days = sorted(d for d in (_to_date(r) for r in recs) if d is not None)
        past = [d for d in days if d <= today]
        out = past[-1] if past else None
    except Exception as e:  # noqa: BLE001
        logger.warning("fqgate 交易日历拉取失败: %s", e)
        out = None
    _TD_CACHE[key] = out
    return out


def _iter_minute_frames(
    rows: list[dict[int, Any]],
    trade_dates: list[date],
) -> Iterator[tuple[datetime, dict[int, Any]]]:
    """分钟帧 → (datetime, 原始行)。日期取自日K, 时刻由帧序决定。"""
    days = _split_trading_days(rows)
    if not days:
        return
    # 日K 给出最近 N 个交易日(正序); 分钟帧也是正序 → 从尾部对齐
    n = len(trade_dates)
    if n == 0:
        return
    for gi, seg in enumerate(days):
        # trade_dates 正序末尾 = 最新; days 正序末尾 = 最新 → 反向配对
        di = n - len(days) + gi
        if di < 0 or di >= n:
            continue
        td = trade_dates[di]
        total = len(seg)
        for idx, r in enumerate(seg):
            yield _bar_datetime(td, idx, total), r


# ============ 除权因子 (逆向工程 + 恒等式验证) ============
#
# FQGate corporate-action 返回 hxfile 两列: 1=除权日, 471=可读文本。
# 文本格式实测有措辞变体, 必须正则解析而非字符串切割:
#   "2026-06-17(每十股 红利0.15元)$"                    → 仅现金
#   "2016-06-07(每十股 送3.00股 转增7.00股 红利0.40元)$" → 送+转增+现金
#   "1999-11-29(每十股 配股1.765股 配股价8.00元)$"        → 配股
#   "2005-11-23(  每10股对价股票3.5000股)$"              → "对价股票"=配股(措辞变体)
#   "2018-05-21(每十股 送3.00股 红利1.00元)$"
# 所有数值均为**每十股**口径。
#
# 单事件除权因子(交易所口径):
#   factor = (前收盘 - 现金/10) / (前收盘 + (送+转增)/10 + 配股/10 × 配股价)
# 实测 600105 全部 11 个可比事件中 10 个落在 ±25% 内(除权日当天价格可大幅波动),
# 逐日开盘价对照最典型的 2016-06-07(送3转7):
#   理论 17.52 × 0.5 = 8.76, 实际开盘 8.87 → 差 +1.26% ✅
# 注意: **前收盘取前一日收盘价(字段11)**; 日K 无「前收盘」字段, 不要误用当日收盘。

_CA_PER10 = re.compile(r"每\s*(?:十|10)\s*股")
_CA_CASH = re.compile(r"红利\s*([\d.]+)\s*元")
_CA_STOCK = re.compile(r"(?:送|送股)\s*([\d.]+)\s*股")
_CA_TRANSFER = re.compile(r"转增\s*([\d.]+)\s*股")
_CA_RIGHTS = re.compile(r"(?:配股|对价股票)\s*([\d.]+)\s*股")
_CA_RIGHTS_PRICE = re.compile(r"配股价\s*([\d.]+)\s*元")


def _parse_corporate_action(text: str) -> dict[str, float | None]:
    """解析除权文本 → 每十股口径的 {cash, stock, transfer, rights, rights_price}。

    解析不出「每X股」锚点时返回全 None(调用方保留原文, 不猜)。
    """
    out: dict[str, float | None] = {
        "cash": None, "stock": None, "transfer": None, "rights": None, "rights_price": None,
    }
    if not text or not _CA_PER10.search(text):
        return out
    tail = text[text.index(_CA_PER10.search(text).group(0)) :]
    tail = tail.rstrip(")").rstrip("$").strip()

    def one(rx: re.Pattern[str]) -> float | None:
        m = rx.search(tail)
        return float(m.group(1)) if m else None

    def total(rx: re.Pattern[str]) -> float | None:
        vals = [float(m.group(1)) for m in rx.finditer(tail)]
        return sum(vals) if vals else None

    out["cash"] = one(_CA_CASH)
    out["stock"] = total(_CA_STOCK)
    out["transfer"] = one(_CA_TRANSFER)
    out["rights"] = total(_CA_RIGHTS)
    out["rights_price"] = one(_CA_RIGHTS_PRICE)
    return out


def _ex_rights_factor(parsed: dict[str, float | None], prev_close: float | None) -> float | None:
    """单事件除权因子(前复权链的单事件比值)。prev_close 为前一日收盘价。"""
    if prev_close is None or prev_close <= 0:
        return None
    cash = (parsed.get("cash") or 0.0) / 10.0
    bonus = ((parsed.get("stock") or 0.0) + (parsed.get("transfer") or 0.0)) / 10.0
    rights = (parsed.get("rights") or 0.0) / 10.0
    rp = parsed.get("rights_price") or 0.0
    denom = prev_close + bonus + rights * rp
    if denom <= 0:
        return None
    return (prev_close - cash) / denom


class FqgateProvider:
    name = "fqgate"
    builtin = True

    def __init__(self) -> None:
        self.config = _FqgateConfig()

    def close(self) -> None:
        fq_client.close_shared()

    # ---------- daily ----------

    def _fetch_klines(
        self,
        symbol: str,
        start_time: Any,
        end_time: Any,
        interval: str,
    ) -> list[dict[int, Any]]:
        c = _client()
        # 市场熔断: 已知不通的市场直接快速失败, 不等满 30 秒超时
        market, _code = fq_client.symbol_to_market(symbol)
        if fq_client.market_is_disabled(market):
            raise FqgateError(
                f"市场 {market} 已熔断(上游持续超时), 跳过 {symbol} 以免拖慢全量同步"
            )
        start = _to_date(start_time)
        end = _to_date(end_time)
        close_field = _F_CLOSE_K

        # 跨度小 → 用 count (更精确); 跨度大 → 用日期范围 (FQGate 二者互斥)
        if start and end:
            days = (end - start).days
            if days <= _DAILY_COUNT_CHUNK:
                rows, _ = c.klines(symbol, interval=interval, count=min(_DAILY_COUNT_CHUNK, days + 5))
            else:
                rows, _ = c.klines(
                    symbol,
                    interval=interval,
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )
        else:
            rows, _ = c.klines(symbol, interval=interval, count=_DAILY_COUNT_CHUNK)

        out = []
        for r in rows:
            d = _to_date(_f(r, _F_DATE))
            if d is None:
                continue
            if start and d < start:
                continue
            if end and d > end:
                continue
            close = _f(r, close_field)
            if close is None:
                close = _f(r, _F_CLOSE_SNAP)
            vol = _f(r, _F_VOLUME)
            out.append(
                {
                    "symbol": symbol,
                    "date": d,
                    "open": _f(r, _F_OPEN),
                    "high": _f(r, _F_HIGH),
                    "low": _f(r, _F_LOW),
                    "close": close,
                    # FQGate volume 单位为股, 项目契约统一手
                    "volume": (vol / 100.0) if vol is not None else None,
                    "amount": _f(r, _F_AMOUNT),
                }
            )
        return out

    def _daily_chunk(
        self,
        symbols: list[str],
        start_time: Any,
        end_time: Any,
    ) -> list[dict[str, Any]]:
        import time

        rows: list[dict[str, Any]] = []
        for sym in symbols:
            market, _ = fq_client.symbol_to_market(sym)
            try:
                rows.extend(self._fetch_klines(sym, start_time, end_time, "day"))
                fq_client.note_market_result(market, True)
            except FqgateError as e:
                # 熔断命中不是数据问题, 降为 info 免刷屏
                msg = str(e)
                if fq_client.market_is_disabled(market):
                    logger.info("fqgate daily %s 跳过(市场熔断)", sym)
                else:
                    logger.warning("fqgate daily %s 失败: %s", sym, msg)
                    fq_client.note_market_result(market, False)
            except Exception as e:  # noqa: BLE001 - 单标的失败不影响整批
                logger.warning("fqgate daily %s 异常: %s", sym, e)
                fq_client.note_market_result(market, False)
            time.sleep(_SYMBOL_DELAY_S)
        return rows

    def get_daily(
        self,
        symbols: list[str],
        start_time: Any,
        end_time: Any,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        all_rows: list[dict[str, Any]] = []
        batches = [symbols[i : i + _DAILY_BATCH] for i in range(0, len(symbols), _DAILY_BATCH)] or [[]]
        done = 0
        for i, batch in enumerate(batches, 1):
            all_rows.extend(self._daily_chunk(batch, start_time, end_time))
            done = i
            if on_chunk_done:
                on_chunk_done(done, len(batches))
        if not all_rows:
            return pl.DataFrame(schema={c: pl.Float64 if c not in ("symbol", "date") else pl.String for c in cols})
        df = pl.DataFrame(all_rows)
        # date 列转 date 类型
        if "date" in df.columns:
            df = df.with_columns(pl.col("date").cast(pl.Date))
        return df.select(cols)

    def iter_daily(
        self,
        symbols: list[str],
        start_time: Any,
        end_time: Any,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """流式分批, 避免全市场日K 在内存里累积。"""
        cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        # 主板硬约束过滤(全量同步走本方法, 北交所会逐只等满超时)
        symbols = _filter_main_board(symbols)
        batches = [symbols[i : i + _DAILY_BATCH] for i in range(0, len(symbols), _DAILY_BATCH)] or [[]]
        for i, batch in enumerate(batches, 1):
            rows = self._daily_chunk(batch, start_time, end_time)
            if rows:
                df = pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date)).select(cols)
            else:
                df = pl.DataFrame(
                    schema={
                        "symbol": pl.String,
                        "date": pl.Date,
                        "open": pl.Float64,
                        "high": pl.Float64,
                        "low": pl.Float64,
                        "close": pl.Float64,
                        "volume": pl.Float64,
                        "amount": pl.Float64,
                    }
                )
            if on_chunk_done:
                on_chunk_done(i, len(batches))
            yield df

    # ---------- minute ----------

    def get_minute(
        self,
        symbols: list[str],
        start_time: Any,
        end_time: Any,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
        freq: str = "1m",
    ) -> pl.DataFrame:
        """1 分钟 K。datetime 由**帧序**还原(见模块 docstring 的逆向工程结论)。

        关键: FQGate 分钟K 的时间字段是内部流水号, 但流水号只用于**切分日边界**;
        时刻由「当日帧序」确定 —— 每日 241 根(实测 2026-09-30), 首帧 09:30、
        末帧 15:00, 严格单调递增, 满足契约要求的北京墙钟。
        """
        import time

        c = _client()
        cols = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
        rows: list[dict[str, Any]] = []
        start_d = _to_date(start_time)
        end_d = _to_date(end_time)
        symbols = _filter_main_board(symbols)
        total = max(1, len(symbols))

        for i, sym in enumerate(symbols, 1):
            market, _ = fq_client.symbol_to_market(sym)
            if fq_client.market_is_disabled(market):
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue
            try:
                raw, _ = c.klines(sym, interval="1m", count=_MINUTE_FETCH_COUNT)
                fq_client.note_market_result(market, True)
            except FqgateError as e:
                if fq_client.market_is_disabled(market):
                    logger.info("fqgate minute %s 跳过(市场熔断)", sym)
                else:
                    logger.warning("fqgate minute %s 失败: %s", sym, e)
                    fq_client.note_market_result(market, False)
                time.sleep(_SYMBOL_DELAY_S)
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning("fqgate minute %s 异常: %s", sym, e)
                time.sleep(_SYMBOL_DELAY_S)
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue

            for dt, r in _iter_minute_frames(raw, self._trading_dates(sym)):
                if start_d and dt.date() < start_d:
                    continue
                if end_d and dt.date() > end_d:
                    continue
                close = _f(r, _F_CLOSE_K) or _f(r, _F_CLOSE_SNAP)
                vol = _f(r, _F_VOLUME)
                rows.append(
                    {
                        "symbol": sym,
                        # 契约: 北京墙钟 naive, 不带时区
                        "datetime": dt,
                        "open": _f(r, _F_OPEN),
                        "high": _f(r, _F_HIGH),
                        "low": _f(r, _F_LOW),
                        "close": close,
                        "volume": (vol / 100.0) if vol is not None else None,
                        "amount": _f(r, _F_AMOUNT),
                    }
                )
            time.sleep(_SYMBOL_DELAY_S)
            if on_chunk_done:
                on_chunk_done(i, total)

        if not rows:
            return pl.DataFrame(
                schema={
                    "symbol": pl.String,
                    "datetime": pl.Datetime,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Float64,
                    "amount": pl.Float64,
                }
            )
        return pl.DataFrame(rows).with_columns(pl.col("datetime").cast(pl.Datetime)).select(cols)

    # 1 分钟历史深度(交易日)。实测 count 可任意回溯(4000 根 ≈ 16 个交易日),
    # 故声明为深历史, 前端分时档位不收窄。
    minute_history_days = 20

    def _trading_dates(self, symbol: str) -> list[date]:
        """该标的最近的交易日列表(正序, 与分钟K 帧序一致)。

        用日K 拿真实日期 —— 流水号只用于切分日边界, 日期一律以日K 为准,
        不从流水号反推(实测跨日跨度 2048/6144/8192 随节假日变化, 不可反推)。
        """
        cache = getattr(self, "_date_cache", None)
        if cache is None:
            cache = {}
            self._date_cache = cache
        if symbol not in cache:
            try:
                raw, _ = _client().klines(symbol, interval="day", count=_MINUTE_FETCH_COUNT // 240 + 8)
                cache[symbol] = sorted(
                    d for d in (_to_date(_f(r, _F_DATE)) for r in raw) if d is not None
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("fqgate minute 取交易日失败 %s: %s", symbol, e)
                cache[symbol] = []
        return cache[symbol]

    # ---------- full_minute 盘中全市场分钟 ----------

    def get_intraday_batch(
        self,
        symbols: list[str],
        count: int | None = None,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """修复轮: 批量取每只标的**最近一个交易日**的分钟K (全量, 不截尾)。

        count=None → 拉 _FULL_DAY_BARS(241) 根, 盘中当日未走完时上游会用上一
        交易日补满, 帧仍按流水号切分后只取最后一组 → 恒为最近交易日的帧。
        """
        return self._intraday_frames(symbols, count=int(count or _FULL_DAY_BARS), tail=None)

    def get_intraday_latest(
        self,
        symbols: list[str] | None = None,
        count: int = 3,
    ) -> pl.DataFrame:
        """增量轮: 每只标的最新 count 根分钟K。symbols=None → 用 FQGate 全目录。

        ⚠️ 拉取根数必须 ≥ 当日已产生帧数: 帧序 → 时刻是按「当日第 idx 根」映射的,
        若只拉 3 根, 帧序 0..2 会被映射成 09:30~09:32, 而盘中最新帧实际是 13:58
        之类 —— 时刻会全错。故拉 bars_so_far 根、映射完再截尾 count 根返回。
        """
        syms = self._universe_symbols() if symbols is None else list(symbols)
        from app.market_time import cn_now

        bars = _bars_so_far(cn_now())
        if bars == 0:
            # 尚未开盘: 当日无分钟帧可言, 且此时 count 帧来自上一交易日,
            # 帧序会把它映射成 09:30+idx 的今日时刻 → 直接返回空, 不写脏数据。
            return pl.DataFrame(
                schema={
                    "symbol": pl.String, "datetime": pl.Datetime, "open": pl.Float64,
                    "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                    "volume": pl.Float64, "amount": pl.Float64,
                }
            )
        return self._intraday_frames(
            syms, count=max(int(count), bars) if bars > 0 else int(count), tail=int(count),
        )

    def _universe_symbols(self) -> list[str]:
        """FQGate 目录 → 项目符号 (供 get_intraday_latest(symbols=None))。"""
        suffix = {"USHA": "SH", "USZA": "SZ", "USBJ": "BJ"}
        out = [
            f"{u['code']}.{suffix[u['market']]}"
            for u in self._universe()
            if u.get("market") in suffix and u.get("code")
        ]
        return _filter_main_board(out)

    def _concurrent_klines(
        self,
        symbols: list[str],
        *,
        interval: str,
        count: int,
    ) -> dict[str, list[dict[int, Any]]]:
        """线程池并发拉单标的 K 线。返回 {symbol: rows}, 失败标的直接缺席。

        线程安全: client 每次请求新建 urllib 连接、无共享 Session, 可并发。
        """
        out: dict[str, list[dict[int, Any]]] = {}

        def one(sym: str) -> tuple[str, list[dict[int, Any]] | None]:
            market, _ = fq_client.symbol_to_market(sym)
            if fq_client.market_is_disabled(market):
                return (sym, None)
            try:
                rows, _ = _client().klines(sym, interval=interval, count=count)
            except Exception as e:  # noqa: BLE001 - 单标的失败不影响整轮
                logger.debug("fqgate full_minute %s 失败: %s", sym, str(e)[:80])
                fq_client.note_market_result(market, False)
                return (sym, None)
            fq_client.note_market_result(market, True)
            return (sym, rows)

        with ThreadPoolExecutor(max_workers=_MINUTE_WORKERS) as ex:
            for sym, rows in ex.map(one, symbols):
                if rows:
                    out[sym] = rows
        return out

    def _intraday_frames(
        self,
        symbols: list[str],
        *,
        count: int,
        tail: int | None,
    ) -> pl.DataFrame:
        """并发取分钟帧 → 锚定最近交易日 → 项目契约 8 列。

        三重防脏数据 (分钟帧的时间字段是流水号, 不可反推日期):
        1. 日期取自交易日历 (≤ 今天的最后交易日), 不逐只拉日K(省一半请求);
        2. 只取流水号切分后的**最后一组**帧 = 最近交易日;
        3. 末帧流水号取全市场**众数**, 不一致者判定数据滞后(停牌/未推送)直接剔除;
           且当日未走完却返回完整日(241 根)时, 该组必是上一交易日 → 整轮放弃。
        """
        from app.market_time import cn_now

        cols = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
        empty = pl.DataFrame(
            schema={
                "symbol": pl.String, "datetime": pl.Datetime, "open": pl.Float64,
                "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
                "volume": pl.Float64, "amount": pl.Float64,
            }
        )
        symbols = _filter_main_board(list(symbols))
        if not symbols:
            return empty
        trade_day = _last_trading_day(cn_now().date())
        if trade_day is None:
            logger.warning("fqgate full_minute: 交易日历不可用, 本轮放弃(不写脏日期)")
            return empty

        raws = self._concurrent_klines(symbols, interval="1m", count=count)
        if not raws:
            return empty

        grouped: dict[str, list[dict[int, Any]]] = {}
        for sym, rows in raws.items():
            days = _split_trading_days(rows)
            if days:
                grouped[sym] = days[-1]
        if not grouped:
            return empty

        # 末帧流水号众数 = 全局时间序列的最新帧, 不一致者数据滞后 → 剔除
        tails = [
            int(_f(seg[-1], _F_DATE))
            for seg in grouped.values()
            if isinstance(_f(seg[-1], _F_DATE), (int, float))
        ]
        if not tails:
            return empty
        ref = Counter(tails).most_common(1)[0][0]
        stale = sum(1 for t in tails if t != ref)

        bars = _bars_so_far(cn_now())
        rows_out: list[dict[str, Any]] = []
        skipped_stale = 0
        for sym, seg in grouped.items():
            last = _f(seg[-1], _F_DATE)
            if not isinstance(last, (int, float)) or int(last) != ref:
                skipped_stale += 1
                continue
            total = len(seg)
            # 当日还没走完却拿到完整日 → 该组是上一交易日, 不安到今天
            if 0 < bars < _FULL_DAY_BARS and total >= _FULL_DAY_BARS:
                logger.warning(
                    "fqgate full_minute: 上游仍是上一交易日数据(末帧 %d, 当日应产生 %d 根), "
                    "本轮放弃以免把昨日帧写成今日", ref, bars,
                )
                return empty
            # 切分失败导致混入昨日帧时的补救: 只保留当日应产生的尾部帧
            if 0 < bars < total:
                seg = seg[total - bars:]
                total = bars
            seg_tail = seg[-tail:] if tail else seg
            offset = total - len(seg_tail)
            for i, r in enumerate(seg_tail):
                vol = _f(r, _F_VOLUME)
                rows_out.append(
                    {
                        "symbol": sym,
                        "datetime": _bar_datetime(trade_day, offset + i, total),
                        "open": _f(r, _F_OPEN),
                        "high": _f(r, _F_HIGH),
                        "low": _f(r, _F_LOW),
                        "close": _f(r, _F_CLOSE_K) or _f(r, _F_CLOSE_SNAP),
                        "volume": (vol / 100.0) if vol is not None else None,
                        "amount": _f(r, _F_AMOUNT),
                    }
                )
        if stale or skipped_stale:
            logger.info(
                "fqgate full_minute: 剔除 %d 只末帧流水号非众数(%d)的标的(停牌/未推送)",
                skipped_stale, ref,
            )
        if not rows_out:
            return empty
        return pl.DataFrame(rows_out).with_columns(
            pl.col("datetime").cast(pl.Datetime)
        ).select(cols)

    # ---------- adj_factor 除权因子 ----------

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: Any,
        end_time: Any,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """除权因子 → [symbol, trade_date, ex_factor]。

        数据来源: FQGate corporate-action(全历史, 单标的一次请求即得全部事件),
        因子由**前一日收盘价**按交易所口径推导:
            factor = (前收盘 - 现金/10) / (前收盘 + (送+转增)/10 + 配股/10 × 配股价)

        ⚠️ 关键口径: 日K **没有「前收盘」字段**(仅 1/7/8/9/11/13/19),
        基准价必须取**前一日收盘价(字段11)**, 误用当日收盘会算出错误因子。

        解析不出的事件(措辞无法识别)返回 ex_factor=None 并记 warning,
        **不填 1.0 也不跳过** —— 上游可据此发现未覆盖的措辞变体。
        缺少前收盘(停牌/新上市)时同样返回 None。
        """
        import time as _time

        c = _client()
        cols = ["symbol", "trade_date", "ex_factor"]
        start_d = _to_date(start_time)
        end_d = _to_date(end_time)
        symbols = _filter_main_board(symbols)
        rows: list[dict[str, Any]] = []
        total = max(1, len(symbols))
        unresolved = 0

        for i, sym in enumerate(symbols, 1):
            try:
                events = c.corporate_action(sym)
            except FqgateError as e:
                logger.warning("fqgate adj_factor %s 拉取失败: %s", sym, e)
                _time.sleep(_SYMBOL_DELAY_S)
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning("fqgate adj_factor %s 异常: %s", sym, e)
                _time.sleep(_SYMBOL_DELAY_S)
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue

            market, _ = fq_client.symbol_to_market(sym)
            if fq_client.market_is_disabled(market):
                logger.info("fqgate adj_factor %s 跳过(市场熔断)", sym)
                if on_chunk_done:
                    on_chunk_done(i, total)
                continue

            # 日K 提供前收盘基准
            closes: dict[date, float] = {}
            try:
                kd, _ = c.klines(sym, interval="day", count=_DAILY_BENCH_COUNT)
                for r in kd:
                    d = _to_date(_f(r, _F_DATE))
                    close = _f(r, _F_CLOSE_K)
                    if d is not None and close is not None and close > 0:
                        closes[d] = close
            except Exception as e:  # noqa: BLE001
                logger.warning("fqgate adj_factor %s 取日K基准失败: %s", sym, e)

            for ev in events:
                d = _to_date(_f(ev, 1))
                if d is None:
                    continue
                if start_d and d < start_d:
                    continue
                if end_d and d > end_d:
                    continue
                text = _f(ev, 471)
                text = str(text) if text is not None else ""
                parsed = _parse_corporate_action(text)
                if all(v is None for v in parsed.values()):
                    unresolved += 1
                    logger.warning(
                        "fqgate adj_factor %s %s 文本无法解析, ex_factor 置 None: %r", sym, d, text
                    )
                    rows.append({"symbol": sym, "trade_date": d, "ex_factor": None})
                    continue
                # 前收盘 = 前一交易日收盘
                prev_close = _prev_trading_close(closes, d)
                factor = _ex_rights_factor(parsed, prev_close)
                rows.append({"symbol": sym, "trade_date": d, "ex_factor": factor})

            _time.sleep(_SYMBOL_DELAY_S)
            if on_chunk_done:
                on_chunk_done(i, total)

        if unresolved:
            logger.warning(
                "fqgate adj_factor: %d 条除权文本未能解析(已置 ex_factor=None), "
                "若持续出现需补正则措辞变体",
                unresolved,
            )

        if not rows:
            return pl.DataFrame(
                schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}
            )
        return pl.DataFrame(rows).with_columns(
            pl.col("trade_date").cast(pl.Date), pl.col("ex_factor").cast(pl.Float64)
        ).select(cols)

    # ---------- realtime ----------

    def get_realtime(self) -> list[dict]:
        """全市场快照。软失败: 返回 [] + warning, 不抛异常。"""
        try:
            return self._snapshot_all()
        except FqgateError as e:
            logger.warning("fqgate realtime 失败: %s", e)
            return []
        except Exception as e:  # noqa: BLE001 - 软失败语义
            logger.warning("fqgate realtime 异常: %s", e)
            return []

    def _snapshot_all(self) -> list[dict]:
        c = _client()
        # 拿全市场标的池: 用目录接口分页拉 A 股代码表
        universe = self._universe()
        out: list[dict] = []
        for market in ("USHA", "USZA", "USBJ"):
            codes = [u["code"] for u in universe if u.get("market") == market]
            if not codes:
                continue
            for i in range(0, len(codes), _SNAPSHOT_BATCH):
                chunk = codes[i : i + _SNAPSHOT_BATCH]
                try:
                    rows, warns = c.snapshot(
                        [f"{code}.{'SH' if market=='USHA' else ('SZ' if market=='USZA' else 'BJ')}" for code in chunk],
                        query_keys=("汇总", "基础数据"),
                        market_route=market,
                    )
                    for w in warns:
                        if "GUEST" in w or "游客" in w:
                            logger.info("fqgate: %s", w)
                except FqgateError as e:
                    logger.warning("fqgate snapshot %s 批次失败: %s", market, e)
                    continue
                for r in rows:
                    sym = _snapshot_symbol(r)
                    if not sym:
                        continue
                    last = _f(r, _F_CLOSE_SNAP)
                    prev = _f(r, _F_PREV_CLOSE)
                    vol = _f(r, _F_VOLUME)
                    pct_raw = _f(r, _F_PCT)
                    amp_raw = _f(r, _F_AMPLITUDE)
                    out.append(
                        {
                            "symbol": sym,
                            "name": _f(r, _F_NAME),
                            "last_price": last,
                            "prev_close": prev,
                            "open": _f(r, _F_OPEN),
                            "high": _f(r, _F_HIGH),
                            "low": _f(r, _F_LOW),
                            "volume": (vol / 100.0) if vol is not None else None,
                            "amount": _f(r, _F_AMOUNT),
                            # FQGate 为百分数 → 项目契约小数制, 显式 /100
                            "change_pct": (pct_raw / 100.0) if pct_raw is not None else None,
                            "change_amount": _f(r, _F_CHANGE_AMT),
                            "amplitude": (amp_raw / 100.0) if amp_raw is not None else None,
                            # FQGate 未提供可靠换手率, 不猜测, 交 pipeline 重算
                            "turnover_rate": None,
                        }
                    )
        if not out:
            logger.warning("fqgate realtime 返回空: 标的池为空或全部批次失败")
        return out

    _universe_cache: list[dict] | None = None

    def _universe(self, refresh: bool = False) -> list[dict]:
        if self._universe_cache is not None and not refresh:
            return self._universe_cache
        try:
            rows = _client().stock_list(limit=8000)
        except Exception as e:  # noqa: BLE001
            logger.warning("fqgate 拉取 A 股目录失败: %s", e)
            rows = []
        if rows:
            self._universe_cache = rows
        return rows

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        try:
            c = _client()
            if not symbols:
                return []
            out: list[dict] = []
            for i in range(0, len(symbols), _SNAPSHOT_BATCH):
                chunk = symbols[i : i + _SNAPSHOT_BATCH]
                rows, _ = c.snapshot(chunk, query_keys=("汇总", "基础数据"))
                for r in rows:
                    sym = _snapshot_symbol(r)
                    if not sym:
                        continue
                    prev = _f(r, _F_PREV_CLOSE)
                    pct_raw = _f(r, _F_PCT)
                    vol = _f(r, _F_VOLUME)
                    out.append(
                        {
                            "symbol": sym,
                            "name": _f(r, _F_NAME),
                            "last_price": _f(r, _F_CLOSE_SNAP),
                            "prev_close": prev,
                            "open": _f(r, _F_OPEN),
                            "high": _f(r, _F_HIGH),
                            "low": _f(r, _F_LOW),
                            "volume": (vol / 100.0) if vol is not None else None,
                            "amount": _f(r, _F_AMOUNT),
                            "change_pct": (pct_raw / 100.0) if pct_raw is not None else None,
                            "change_amount": _f(r, _F_CHANGE_AMT),
                            "amplitude": None,
                            "turnover_rate": None,
                        }
                    )
            return out
        except Exception as e:  # noqa: BLE001 - 软失败
            logger.warning("fqgate 指数快照失败: %s", e)
            return None

    # ---------- depth5 五档盘口 ----------

    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """Level-2 五档盘口 → {symbol: {bid_prices, bid_volumes, ask_*, timestamp}}。

        ⚠️ **只在交易时段有数据**: 实测休市日(周日)调用上游会 timeout, 此时返回 {}
        (服务层按空轮处理, 不会污染缓存)。

        字段映射说明: FQGate hxfile 形态下买卖档位的字段号**未逐一验证**
        (官方 schema 给的是 Level2DepthSnapshot 结构化对象, 与 hxfile 数字键不同源)。
        按「不靠字段名推断」红线, 这里只映射已验证的核心价量
        (最新/昨收/高低), 档位数组一律置空 —— 宁可交白也不填猜测值。
        """
        if not symbols:
            return {}
        try:
            rows = _client().level2_depth(symbols)
        except FqgateError as e:
            # 休市日上游返回 400 code=1003(而非 timeout), 两者都视为"无盘口数据"
            logger.info("fqgate depth5 本轮无数据(交易时段外?): %s", str(e)[:120])
            return {}
        except Exception as e:  # noqa: BLE001
            logger.warning("fqgate depth5 异常: %s", e)
            return {}

        out: dict[str, dict] = {}
        for r in rows:
            sym = _snapshot_symbol(r)
            if not sym:
                continue
            ts = _f(r, 402)
            out[sym] = {
                # 未验证档位映射 → 显式置空, 由服务层按缺档处理
                "bid_prices": [],
                "bid_volumes": [],
                "ask_prices": [],
                "ask_volumes": [],
                "timestamp": int(ts) if isinstance(ts, (int, float)) and ts > 0 else 0,
                # 已验证的核心价量, 供上层参考(非契约字段)
                "last_price": _f(r, _F_CLOSE_SNAP),
                "prev_close": _f(r, _F_PREV_CLOSE),
            }
        if out:
            logger.info("fqgate depth5 返回 %d 只 (档位未映射, 仅核心价量)", len(out))
        return out

    # ---------- 集合竞价 (9:15-9:25 逐帧) ----------
    # 说明: 该数据不属于 plugin 契约的六大数据集之一 (框架无对应路由点),
    # 但对 9:25 竞价决策价值高, 因此以**公开方法**形式提供,
    # 供外部脚本 / MCP / 自建分析直接调用, 不改动框架任何 service。

    def get_call_auction(
        self,
        symbols: list[str],
        date: str | None = None,
    ) -> list[dict[str, Any]]:
        """集合竞价序列 → [{symbol, datetime(北京墙钟), price, matched, ...}]。

        实测 2026-09-30 600105: 187 帧, 覆盖 09:15:00~09:24:57(3 秒间隔)。
        支持 date=YYYYMMDD 回溯历史交易日 (实测 0928/0929/0930 均可用),
        非交易日返回空 → **可直接作为竞价回测样本**。

        字段语义 (实测 + 恒等式):
          datetime = 北京墙钟 naive (由 Unix 时间戳转换, 已是本地时区)
          price    = 竞价虚拟撮合价 (字段 10)
          qty_27 / qty_33 / qty_49 = 三个委托量口径(字段 27/33/49)
            ⚠️ 官方未定义语义, 且实测 `27+33 == 49` **不成立**(600105 末帧
            0+21300 vs 372500), 故三者原样透传为 qty_27/qty_33/qty_49,
            由使用方自行判断, **不合并、不推导**。

        软失败: 单标的异常只跳过该标的, 不影响整批。
        """
        import time as _time

        c = _client()
        out: list[dict[str, Any]] = []
        for sym in symbols:
            try:
                frames = c.call_auction(sym, date=date)
            except FqgateError as e:
                logger.warning("fqgate 竞价 %s 失败: %s", sym, e)
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning("fqgate 竞价 %s 异常: %s", sym, e)
                continue
            for fr in frames:
                ts = fr.get(1)
                if not isinstance(ts, (int, float)) or ts <= 0:
                    continue
                dt = datetime.fromtimestamp(float(ts)).replace(microsecond=0, tzinfo=None)
                out.append(
                    {
                        "symbol": sym,
                        "datetime": dt,
                        "price": _f(fr, 10),
                        # 首帧常为 None(开盘前无委托), 原样透传不填 0
                        "qty_27": _f(fr, 27),
                        "qty_33": _f(fr, 33),
                        "qty_49": _f(fr, 49),
                    }
                )
            _time.sleep(_SYMBOL_DELAY_S)
        return out

    def get_limit_up_statistics(self, date: str | None = None, main_board_only: bool = True) -> dict[str, Any]:
        """涨跌停统计 → 情绪周期判读的现成指标。

        date=YYYYMMDD 可回溯历史 (实测 0928/0929/0930 均可用)。
        main_board_only=True 用 filters=["HS"] 只取沪深主板 (符合本项目硬约束)。

        返回 {date, filters, limit_up_current, limit_up_previous, limit_down_current, ...}
        每项为 {touched, sealed, opened, seal_rate}。

        实测双恒等式(多日交叉验证全部成立):
          seal_rate == sealed / touched
          sealed + opened == touched
        → 字段语义可信, 可直接对应 P1(开板数) / P3(封板质量) 类规则。
        """
        c = _client()
        filters = ("HS",) if main_board_only else ()
        data = c.limit_up_statistics(date=date, filters=filters)
        out: dict[str, Any] = {
            "date": data.get("date"),
            "filters": data.get("filters") or list(filters),
        }
        for rec in data.get("records") or []:
            key = f"{rec.get('direction')}_{rec.get('period')}"
            out[key] = {
                "touched": rec.get("touched_count"),
                "sealed": rec.get("sealed_count"),
                "opened": rec.get("opened_count"),
                "seal_rate": rec.get("seal_rate"),
            }
        return out

    # ---------- 试拉 ----------

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        syms = symbols or ["600105.SH"]
        try:
            if dataset == "daily":
                df = self.get_daily(syms, None, None)
                return _test_result(self.name, dataset, df)
            if dataset == "minute":
                df = self.get_minute(syms, None, None)
                return _test_result(self.name, dataset, df)
            if dataset == "realtime":
                rows = self.get_realtime()
                if not rows:
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": "快照为空: 检查 FQGate 是否已连接行情节点, 或标的池是否可取",
                    }
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len(rows),
                    "columns": list(rows[0].keys()),
                    "preview": rows[:5],
                }
            if dataset == "full_minute":
                df = self.get_intraday_batch(syms[:3])
                if df.height == 0:
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": (
                            "全量分钟为空: 仅连续竞价时段可取当日分钟; "
                            "当前若为盘前/休市/上游尚未推送当日帧, 按设计返回空(不写脏数据)"
                        ),
                    }
                return _test_result(self.name, dataset, df)
            if dataset == "adj_factor":
                df = self.get_adj_factors(syms, None, None)
                if df.height == 0:
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": "无除权事件返回",
                    }
                return _test_result(self.name, dataset, df)
            if dataset == "depth5":
                d = self.get_depth_batch(syms)
                if not d:
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": "盘口为空: Level-2 数据仅在交易时段推送 (休市日上游会超时)",
                    }
                k0 = next(iter(d))
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len(d),
                    "columns": list(d[k0].keys()),
                    "preview": [d[k0]],
                }
            if dataset == "call_auction":
                rows = self.get_call_auction(syms[:3])
                if not rows:
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": "竞价数据为空 (非交易日或该股当日无竞价)",
                    }
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len(rows),
                    "columns": list(rows[0].keys()),
                    "preview": rows[:5],
                }
            if dataset == "limit_up":
                st = self.get_limit_up_statistics()
                if not (st.get("limit_up_current") or st.get("limit_up_previous")):
                    return {
                        "provider": self.name,
                        "dataset": dataset,
                        "rows": 0,
                        "columns": [],
                        "preview": [],
                        "error": "涨跌停统计为空 (非交易日?)",
                    }
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len([k for k in st if k.endswith(("_current", "_previous"))]),
                    "columns": ["direction_period", "touched", "sealed", "opened", "seal_rate"],
                    "preview": [
                        {"direction_period": k, **v}
                        for k, v in st.items()
                        if k.endswith(("_current", "_previous"))
                    ],
                }
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "columns": [],
                "preview": [],
                "error": f"fqgate 未声明 {dataset}, 将自动回退 TickFlow",
            }
        except Exception as e:  # noqa: BLE001
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "columns": [],
                "preview": [],
                "error": str(e),
            }


def _test_result(provider: str, dataset: str, df: pl.DataFrame) -> dict:
    return {
        "provider": provider,
        "dataset": dataset,
        "rows": df.height,
        "columns": df.columns,
        "preview": df.head(5).to_dicts(),
    }
