"""自选列表「盘中增强」服务 —— 竞价涨幅 / 最相关概念板块。

两者都依赖 fqgate(本机同花顺), 由本服务统一:
  1. 拉取 (带进程内缓存, 避免每帧都打上游)
  2. 软失败 —— 上游不可用时返回空/None, **绝不阻塞自选列表主查询**
  3. 失败可观测 —— 返回 warnings, 由 API 层透出给前端自披露

## 竞价涨幅口径 (实测 2026-10-05 恒等式验证)
fqgate `/v1/market/history/call-auction` 的**末帧(≈09:24:57/09:24:59)字段 10**
即 09:25 集合竞价定价 —— 用 4 只股对照当日日K 开盘价验证:
  600105 37.49 vs 37.47 (+0.05%) / 000001 11.36 vs 11.36 (0.00%)
  600519 1238.0 vs 1239.53 (-0.12%) / 601398 8.17 vs 开盘 (一致)
差异在开盘撮合的正常波动范围内, 确认该字段即竞价定价。

竞价涨幅 = (竞价价 - 前一交易日收盘) / 前一交易日收盘 x 100
**只有在 09:25 竞价结束后才有值**; 未到该时点/非交易日/上游无数据 → None(前端渲染 "—")。

## 最相关概念板块口径
fqgate `/v1/market/catalog/security-blocks`(个股 → 所属板块, 单次请求即得全部)
返回 `format=gbk_string`, records 形如 (以贵州茅台实测):
  `81,贵州,URFI882010,34,2;203,上证50,,50,0;56035,白酒概念,URFI885525,48,3;...`
每条 5 字段: `板块编号, 板块名称, link_code, 成分数, 类型码`
  - **类型码 3 = 概念板块**(同花顺概念)
  - 类型码 1 = 行业板块(申万类)
  - 类型码 2 = 地区板块
  - link_code 为空 ⇒ 该板块无对应 link(仅作分类标记)

「最相关概念」= **概念板块(类型码 3)中成分股数最少者** —— 成分越少越精准专一,
成分 thousands 的(如"融资融券"3876/ "沪股通"1644)属泛标签, 不反映题材归属。
该口径完全基于真实接口字段, 无任何推测成分, 且已用 4 只股人工核对:
  贵州茅台 → 白酒概念(48) 而非 融资融券(3876)
  永鼎股份 → 光纤概念(116)
并列时按板块名排序保证稳定。

## ⚠️ symbol 写法坑 (2026-10-05 实测, 统一入口已内联到 client)
`block-constituents` 字段 5 返回 `USZA002304` 这种「4位市场码 + 6位代码」,
而项目内部 symbol 一律是 `002304.SZ`。两者混用会让命中率**恒为 0**(踩过一次)。
本模块统一走 `client.symbol_to_market()` 做拆分, 不再自己解析前缀。
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# ===== 缓存TTL (秒) =====
# 竞价: 09:15~09:25 数据逐帧变化, 但我们只取末帧定价 → 60s 足够
_AUCTION_TTL_S = 60
# 板块归属: 盘中基本不变 → 30 分钟
_BLOCKS_TTL_S = 1800

_LOCK = threading.Lock()
_cache: dict[str, tuple[float, Any]] = {}


def _cached(key: str, ttl: int, producer):
    now = time.monotonic()
    with _LOCK:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with _LOCK:
        _cache[key] = (time.monotonic(), val)
    return val


def _fingerprint(symbols: list[str]) -> str:
    """symbol 集合指纹 (缓存键用)。

    缓存键**必须包含集合本身**, 不能只用 `len(symbols)`: 自选股增删但总数不变
    (例: 删掉 A 加进 B, 仍是 29 只) 时, 只按长度做键会直接命中上一份缓存,
    把**别的标的**的竞价/板块数据返回给当前列表 —— 违反"数据必须真实"底线。
    概念列 TTL 30min, 命中错缓存的时间窗比竞价列(60s)长得多。
    """
    joined = ",".join(sorted(set(symbols)))
    return hashlib.md5(joined.encode()).hexdigest()[:10]


# ===== 竞价涨幅 =====

def auction_pct_map(symbols: list[str], trade_date: str | None = None) -> tuple[dict[str, float], list[str]]:
    """symbol → 竞价涨幅(%)。返回 (映射, 警告列表)。

    trade_date 形如 "20260930"; 省略则用 fqgate 该接口的默认(当日)。
    非交易日/未到 09:25 → 空映射 + 一条说明性 warning(前端可自披露)。

    ## 性能 (2026-10-05 修复 30s 超时)
    实测休市日单只 `call-auction` 挂满 ~30s 才回 504, 29 只自选串行 = 870s,
    前端 30s 必然超时。两道修复:
      1. **非交易日短路**: 未显式指定 trade_date 且今日非交易日 → 直接返回,
         一个上游请求都不发 (竞价数据当日本就不存在);
      2. **线程池并发** 逐只拉取 (client 每请求新建连接, 无共享 Session, 线程安全)。
    """
    if not symbols:
        return {}, []

    # ---- ① 非交易日短路 (仅在未指定回溯日期时生效) ----
    if not trade_date:
        try:
            from app.services.trading_day import is_trading_day

            if not is_trading_day():
                return {}, ["今日非交易日, 无集合竞价数据 (竞价列显示 —)"]
        except Exception as e:
            logger.warning("交易日探针不可用, 竞价按交易日继续: %s", e)

    def _one(sym: str) -> tuple[str, float | None, str | None]:
        """返回 (symbol, 竞价涨幅, 警告); 失败时涨幅 None。"""
        from datetime import datetime as _dt

        from app.plugins.fqgate.client import FqgateClient, FqgateError, symbol_to_market

        c = FqgateClient(timeout=8)
        dstr = trade_date or _dt.now().strftime("%Y%m%d")
        # 只借 symbol_to_market 做**格式校验**(非法代码直接跳过); 真正请求由
        # call_auction 内部自行拆分 market/code, 故这里不接返回值 (RUF059)。
        try:
            symbol_to_market(sym)
        except Exception:
            return sym, None, None
        try:
            rows = c.call_auction(sym, date=dstr, timeout=8)
        except FqgateError as e:
            return sym, None, f"{sym} 竞价拉取失败: {str(e)[:60]}"
        except Exception as e:
            return sym, None, f"{sym} 竞价异常: {str(e)[:60]}"
        if not rows:
            # 非交易日 / 未到 09:25 —— 不算错误, 但要让用户知道为什么没有值
            return sym, None, None

        last = rows[-1]
        ts = last.get(1)
        auc_px = last.get(10)
        if not auc_px or not isinstance(auc_px, (int, float)) or auc_px <= 0:
            return sym, None, None
        # 时间校验: 只接受 09:15~09:30 的帧, 防止上游返回异常数据
        if isinstance(ts, (int, float)):
            hhmm = _dt.fromtimestamp(ts).strftime("%H%M")
            if not ("0915" <= hhmm <= "0930"):
                return sym, None, f"{sym} 竞价帧时间异常({hhmm}), 已忽略"

        # 昨收: 从日K 取该竞价日之前最近一根的收盘(字段 11)
        prev_close = _prev_close(c, sym, dstr)
        if not prev_close or prev_close <= 0:
            return sym, None, None
        return sym, round((float(auc_px) - prev_close) / prev_close * 100.0, 2), None

    def _produce():
        # ---- ② 线程池并发 (29 只 x 单只 ~0.1s ≈ 亚秒级; 上游异常时也只等一轮超时) ----
        from concurrent.futures import ThreadPoolExecutor

        out: dict[str, float] = {}
        warns: list[str] = []
        dstr = trade_date or "auto"
        workers = min(12, max(4, len(symbols)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="auction") as pool:
            for sym, pct, warn in pool.map(_one, symbols):
                if pct is not None:
                    out[sym] = pct
                if warn:
                    warns.append(warn)

        if not out and not warns:
            warns.append(f"竞价数据为空 (date={dstr}): 可能尚未到 09:25 或当日非交易日")
        return out, warns

    return _cached(f"auction:{trade_date or 'auto'}:{_fingerprint(symbols)}", _AUCTION_TTL_S, _produce)


def _prev_close(c, symbol: str, dstr: str) -> float | None:
    """竞价日之前最近一个交易日的收盘价(日K 字段 11)。"""
    try:
        kd, _ = c.klines(symbol, interval="day", count=6, adjust="None")
    except Exception:
        return None
    dates = sorted(
        (str(r.get(1)) for r in kd if r.get(1) is not None and str(r.get(1)) < dstr),
        reverse=True,
    )
    if not dates:
        return None
    target = dates[0]
    for r in kd:
        if str(r.get(1)) == target:
            v = r.get(11)
            return float(v) if isinstance(v, (int, float)) and v > 0 else None
    return None


# ===== 最相关概念板块 =====

def concepts_of(symbols: list[str], top_n: int = 3) -> tuple[dict[str, list[tuple[str, int]]], list[str]]:
    """symbol → [(概念名, 成分股数), ...] 升序(最精准在前)。返回 (映射, 警告)。

    数据源: fqgate `security-blocks`(单次请求得该股全部所属板块, format=gbk_string)。
    口径见模块 docstring: 类型码 3(概念)按成分股数升序, 排除 >500 的泛标签。
    top_n 截断, 避免单元格过长(前端只显示前 N 个 + `+N`)。
    """
    if not symbols:
        return {}, []

    def _one(sym: str) -> tuple[str, list[tuple[str, int]], str | None]:
        """返回 (symbol, 概念列表, 警告)。"""
        from app.plugins.fqgate.client import FqgateClient, FqgateError, symbol_to_market

        c = FqgateClient(timeout=10)
        try:
            mkt, code = symbol_to_market(sym)
            obj = c._post(
                "/v1/market/catalog/security-blocks",
                {"market": mkt, "code": code},
                timeout=10,
            )
        except FqgateError as e:
            return sym, [], f"{sym} 板块查询失败: {str(e)[:60]}"
        except Exception as e:
            return sym, [], f"{sym} 板块查询异常: {str(e)[:60]}"

        recs = (obj.get("data") or {}).get("records")
        picked = _pick_concepts(recs, top_n) if recs else []
        return sym, picked, None

    def _produce():
        from concurrent.futures import ThreadPoolExecutor

        out: dict[str, list[tuple[str, int]]] = {}
        warns: list[str] = []
        n_fail = 0
        workers = min(12, max(4, len(symbols)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="blocks") as pool:
            for sym, picked, warn in pool.map(_one, symbols):
                if warn:
                    n_fail += 1
                    if n_fail <= 3:
                        warns.append(warn)
                    continue
                if picked:
                    out[sym] = picked

        if not out and not warns:
            warns.append("未取到任何板块归属(个股可能无板块标签或上游目录为空)")
        return out, warns

    return _cached(f"concepts:{_fingerprint(symbols)}:{top_n}", _BLOCKS_TTL_S, _produce)


def _pick_concepts(recs: Any, top_n: int) -> list[tuple[str, int]]:
    """概念板块(类型码 3)按成分股数升序 —— 越少越精准专一。排除泛标签。"""
    cands = [
        (size, name)
        for name, _link, size, kind in _iter_blocks(recs)
        if kind == _BLOCK_KIND_CONCEPT and size <= _GENERIC_MAX_SIZE
    ]
    if not cands:
        return []
    cands.sort(key=lambda x: (x[0], x[1]))   # 成分数升序; 并列按名保证稳定
    return [(name, size) for size, name in cands[:top_n]]


# 板块类型码 (实测 security-blocks 第 5 个字段)
_BLOCK_KIND_CONCEPT = 3

# 泛标签黑名单: 成分股数量级过大, 不反映题材归属 (实测阈值 500)
_GENERIC_MAX_SIZE = 500


def _iter_blocks(recs: Any):
    """records(gbk_string) → [(name, link_code, size, kind)]。

    实测单条形如 `56035,白酒概念,URFI885525,48,3`, 多条以 `;` 分隔。
    link_code 可能为空(仅分类标记)。
    """
    text = "\n".join(str(r) for r in recs) if isinstance(recs, list) else str(recs or "")
    for item in text.split(";"):
        item = item.strip()
        if not item:
            continue
        parts = item.split(",")
        if len(parts) < 5:
            continue
        name = parts[1].strip()
        link = parts[2].strip()
        try:
            size = int(parts[3])
            kind = int(parts[4])
        except ValueError:
            continue
        if not name:
            continue
        yield name, link, size, kind