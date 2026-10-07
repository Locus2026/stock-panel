"""FQGate 客户端层 —— 本机同花顺行情网关 HTTP 桥接。

FQGate 主程序 (127.0.0.1:17281) 把同花顺二进制行情协议封装成 REST API。
本模块只负责:
  1. HTTP 调用 (POST JSON)
  2. hxfile / gbk_string 记录解码 → 扁平 dict
  3. 市场码与内部 symbol 格式互转

设计要点 (实测 2026-10-03, FQGate 1.0.5):
  - market 用同花顺码 (USHA=沪A / USZA=深A / USBJ=北交所 / USHK=港股),
    不是 "cn"/"SH" 这类写法; 传错会拿到 504 或空。
  - interval 用小写 "day"/"1m"; adjust 首字母大写 "None"/"forward"。
  - 响应外壳 {code, message, data:{format, records:[...]}};
    data 为 null 时是业务错误 (code != 0)。
  - records 是**同花顺 hxfile 键值对**: 单行 = [{字段号: {type, value}}, ...],
    日K 一行一天, 快照一行一只股票 (外层 records 是多行数组)。
  - 当前若以游客(guest)登录, 上游会附带 GUEST_MARKET_DATA 警告且有 504 风险;
    不视为错误, 由上层决定是否提示用户登录。

不做单位换算 —— 契约换算集中在 provider.py, 避免口径分裂。
"""

from __future__ import annotations

import contextlib
import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://127.0.0.1:17281"

# 内部 symbol 后缀 → 同花顺 market 码
_SUFFIX_TO_MARKET = {
    "SH": "USHA",
    "SZ": "USZA",
    "BJ": "USBJ",
}
# 同花顺 market 码 → 内部后缀 (含 A 股与常用指数/ETF 市场)
_MARKET_TO_SUFFIX = {
    "USHA": "SH",
    "USZA": "SZ",
    "USBJ": "BJ",
    "USHK": "HK",
}

# 同花顺 market + 内部 asset_type 的组合映射:
# A 股内部 asset_type 恒为 "stock"(含 ETF/指数), fqgate 端点已按市场分路, 无需区分。
_INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "60m",
    "day": "day",
    "1d": "day",
    "week": "week",
    "month": "month",
}

_ADJUST_MAP = {
    "none": "None",
    "": "None",
    "forward": "forward",
    "backward": "backward",
    "qfq": "forward",
    "hfq": "backward",
}


class FqgateError(RuntimeError):
    """FQGate 业务或传输错误。"""


def _default_timeout() -> float:
    import os

    try:
        return float(os.environ.get("FQGATE_TIMEOUT", "30"))
    except ValueError:
        return 30.0


def symbol_to_market(symbol: str) -> tuple[str, str]:
    """内部 symbol (600519.SH) → (market, code)。无后缀时按代码首位推断。"""
    code, _, suffix = symbol.partition(".")
    if not suffix:
        # 无后缀: 6 开头沪, 0/3 开头深, 4/8 开头北交所
        suffix = "SH" if code.startswith("6") else ("BJ" if code.startswith(("4", "8")) else "SZ")
    market = _SUFFIX_TO_MARKET.get(suffix.upper())
    if market is None:
        raise FqgateError(f"不支持的市场后缀: {symbol}")
    return market, code


def market_to_symbol(market: str, code: str) -> str:
    """(market, code) → 内部 symbol。未知市场回退为按代码首位推断。"""
    suffix = _MARKET_TO_SUFFIX.get(market)
    if suffix is None:
        suffix = "SH" if code.startswith("6") else ("BJ" if code.startswith(("4", "8")) else "SZ")
    return f"{code}.{suffix}"


def market_code_of(symbol: str) -> str:
    """FQGate 快照端点按市场分路, 需要纯市场码。"""
    market, _ = symbol_to_market(symbol)
    return market


def _flatten_record(record: Any) -> dict[int, Any]:
    """一条 hxfile 记录 → {字段号(int): value}。

    实测形态: 单层 dict, {"1": {"type":"integer","value":20260928}, "7": {...}}
    """
    if not isinstance(record, dict):
        return {}
    out: dict[int, Any] = {}
    for k, v in record.items():
        try:
            out[int(k)] = v.get("value") if isinstance(v, dict) else v
        except (TypeError, ValueError):
            continue
    return out


def decode_rows(data: Any) -> list[dict[int, Any]]:
    """把 data.records 解成行列表。

    实测形态 (FQGate 1.0.5): records = [段落, ...], 段落 = [行, ...],
    行 = {"字段号": {"type","value"}}。日K一个段落多行, 快照逐段一行。
    """
    if not isinstance(data, dict):
        return []
    records = data.get("records")
    if not isinstance(records, list):
        return []
    rows: list[dict[int, Any]] = []
    for segment in records:
        if isinstance(segment, list):
            for rec in segment:
                row = _flatten_record(rec)
                if row:
                    rows.append(row)
        else:
            row = _flatten_record(segment)
            if row:
                rows.append(row)
    return rows


class FqgateClient:
    """FQGate REST 客户端 (同步, 短超时, 失败即抛)。"""

    def __init__(self, base_url: str | None = None, timeout: float | None = None) -> None:
        import os

        self.base_url = (base_url or os.environ.get("FQGATE_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout if timeout is not None else _default_timeout()

    # ---- 传输 ----

    def _post(self, path: str, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(  # noqa: S310 - 本机回环地址, 固定 http
            url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        t = timeout if timeout is not None else self.timeout
        try:
            with urllib.request.urlopen(req, timeout=t) as resp:  # noqa: S310
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:300] if e.fp else ""
            raise FqgateError(f"HTTP {e.code} {path}: {detail}") from e
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise FqgateError(f"连接 FQGate 失败 {url}: {e}") from e

        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            raise FqgateError(f"响应非 JSON: {raw[:200]}") from e

        # 业务错误: data 为 null 且 code 非 0
        code = obj.get("code")
        if code not in (0, None) and not obj.get("data"):
            raise FqgateError(f"FQGate 业务错误 code={code}: {obj.get('message')}")
        return obj

    @staticmethod
    def warnings_of(obj: dict[str, Any]) -> list[str]:
        return [w.get("message", "") for w in (obj.get("warnings") or []) if isinstance(w, dict)]

    # ---- 健康 ----

    def health(self) -> dict[str, Any]:
        url = f"{self.base_url}/v1/market/health"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:  # noqa: BLE001 - 健康检查永不抛
            return {"code": -1, "message": str(e), "data": {"connected": False}}

    # ---- 目录 ----

    def search_symbols(self, pattern: str, need_market: str | None = None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"pattern": pattern}
        if need_market:
            payload["need_market"] = need_market
        obj = self._post("/v1/market/catalog/search-symbols", payload, timeout=15)
        out = []
        for row in decode_rows(obj.get("data")):
            code = row.get(5)
            name = row.get(55)
            market = row.get(6) or row.get(3)  # 6=市场(快照形态) / 3=市场(搜索形态)
            if isinstance(market, str) and market and code and not str(code).startswith(("US", "ZS")):
                market = "USHA" if str(market) in ("SH", "沪") else market
            if code and name:
                out.append({"code": str(code), "name": str(name), "market": market})
        return out

    def stock_list(self, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        """A 股代码表 (分页)。用于全市场标的池。"""
        rows_out: list[dict[str, Any]] = []
        offset = 0
        while len(rows_out) < limit:
            page = min(500, limit - len(rows_out))
            obj = self._post("/v1/market/catalog/stock-cn", {"limit": page, "offset": offset}, timeout=20)
            rows = decode_rows(obj.get("data"))
            if not rows:
                break
            for row in rows:
                raw = row.get(5)
                name = row.get(55)
                if not raw or not name:
                    continue
                raw = str(raw).strip()
                # 形如 USZA002346 / USBJ430047: 拆同花顺市场码(4位) + 6位代码
                market = raw[:4] if raw[:4] in _MARKET_TO_SUFFIX else None
                code = raw[4:] if market else raw
                # 残缺行(分页边界可能截断)跳过, 不让脏代码进标的池
                if not market or len(code) != 6 or not code.isdigit():
                    logger.debug("fqgate 目录跳过残缺行: %r", raw)
                    continue
                rows_out.append({"code": code, "name": str(name), "market": market})
            offset += len(rows)
        return rows_out[:limit]

    # ---- 行情 ----

    def klines(
        self,
        symbol: str,
        *,
        interval: str = "day",
        adjust: str = "None",
        count: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[list[dict[int, Any]], list[str]]:
        """单标的 K 线。返回 (行列表, 警告列表)。

        count 与 (start_date, end_date) 互斥 —— FQGate 侧明确要求二者不能同时给。
        """
        market, code = symbol_to_market(symbol)
        payload: dict[str, Any] = {
            "market": market,
            "code": code,
            "interval": _INTERVAL_MAP.get(interval, interval),
            "adjust": _ADJUST_MAP.get(adjust, "None"),
        }
        if start_date and end_date:
            payload["start_date"] = start_date
            payload["end_date"] = end_date
        elif count:
            payload["count"] = int(count)
        obj = self._post("/v1/market/history/klines", payload)
        return decode_rows(obj.get("data")), self.warnings_of(obj)

    def trading_days(self, start_date: str, end_date: str) -> list[str]:
        """交易日历 → 升序 'YYYYMMDD' 字符串列表。

        实测 2026-09-01~2026-10-10: 返回 23 个交易日 (09-01~09-30 + 10-08/10-09),
        国庆休市段正确缺失 —— 可用于把分钟帧锚定到真实交易日。
        请求参数格式是 YYYYMMDD (带横线的 ISO 日期会被 400 拒绝)。
        """
        obj = self._post(
            "/v1/market/calendar/trading-days",
            {"start_date": start_date, "end_date": end_date},
        )
        data = obj.get("data") or {}
        return [str(x) for x in (data.get("records") or [])]

    def intraday(self, symbol: str) -> tuple[list[dict[int, Any]], list[str]]:
        """单标的分时(当日)。"""
        market, code = symbol_to_market(symbol)
        obj = self._post("/v1/market/history/intraday", {"market": market, "code": code})
        return decode_rows(obj.get("data")), self.warnings_of(obj)

    def snapshot(
        self,
        symbols: list[str],
        query_keys: tuple[str, ...] = ("汇总",),
        market_route: str | None = None,
    ) -> tuple[list[dict[int, Any]], list[str]]:
        """实时快照。

        query_keys 可给多个字段组, 结果按 symbol 合并。
        实测「汇总」组**不含开盘价**(无字段 7),「基础数据」组含完整 OHLC,
        故全市场快照需两组都取。

        market_route 显式指定时所有 symbol 走该市场(用于按市场批量拉取);
        留空则**按市场自动分组** —— 实测 /realtime/cn 拒绝混合市场请求
        (沪+深混发返回 400 code=1003), 必须分市场发。
        """
        if not symbols:
            return [], []

        # 按市场分组(端点不接受混合市场)
        groups: dict[str, list[str]] = {}
        for s in symbols:
            m, _ = symbol_to_market(s)
            groups.setdefault(market_route or m, []).append(s)

        merged: dict[str, dict[int, Any]] = {}
        order: list[str] = []
        all_warns: list[str] = []

        for market, syms in groups.items():
            secs = []
            for s in syms:
                _, code = symbol_to_market(s)
                secs.append({"market": market, "code": code})

            for qk in query_keys:
                try:
                    obj = self._post(
                        "/v1/market/realtime/cn",
                        {"securities": secs, "query_key": qk},
                    )
                except FqgateError as e:
                    all_warns.append(f"{market}/{qk} 失败: {e}")
                    continue
                all_warns.extend(self.warnings_of(obj))
                for row in decode_rows(obj.get("data")):
                    raw = row.get(5)
                    if raw is None:
                        continue
                    key = f"{raw}"
                    if not key or key == "None":
                        continue
                    # 正式账号下同一只股票会返回多个段落(主行情段 + 附加段,
                    # 附加段只有 4~5 个字段且无名称)。只把**含最新价或昨收的行情段**
                    # 作为该 symbol 的主记录, 其余段落仅补充缺失字段, 避免
                    # 附加段把主记录覆盖成残行。
                    is_quote = 10 in row or 6 in row or 11 in row
                    if key not in order:
                        order.append(key)
                        merged[key] = {}
                    slot = merged[key]
                    if is_quote:
                        for k, v in row.items():
                            slot[k] = v
                    else:
                        for k, v in row.items():
                            slot.setdefault(k, v)
        return [merged[k] for k in order if merged.get(k)], all_warns

    def index_snapshot(self, symbols: list[str]) -> tuple[list[dict[int, Any]], list[str]]:
        return self.snapshot(symbols, query_keys=("汇总", "基础数据"))

    # ---- 集合竞价 (9:15-9:25 逐帧, 字段 1 = 标准 Unix 秒级时间戳) ----

    def call_auction(
        self,
        symbol: str,
        date: str | None = None,
        timeout: float = 10.0,
    ) -> list[dict[str, Any]]:
        """单标的集合竞价序列。

        date 格式 YYYYMMDD, 省略取最近交易日; 非交易日返回空。
        实测可回溯多日 (20260928/29/30 均返回 171~197 帧), 因此可做竞价回测样本。

        帧字段 (实测, 语义以恒等式与官方 schema 为准):
          1  = Unix 时间戳(秒)   10 = 竞价虚拟撮合价
          27 / 33 / 49 = 委托量类指标(官方未定义语义, 见 provider 注释)

        ⚠️ timeout 默认 10s (原硬编码 45s): 休市日实测上游挂满 ~30s 才回
        HTTP 504「行情请求超时」, 客户端 45s 形同虚设; 逐只串行时 29 只自选
        = 870s, 前端 30s 必然超时 (2026-10-05 实测)。竞价数据只在 09:15~09:30
        有意义, 等 30s 毫无价值, 宁可快速失败走空值 + warning。
        """
        market, code = symbol_to_market(symbol)
        payload: dict[str, Any] = {"market": market, "code": code}
        if date:
            payload["date"] = date.replace("-", "")
        obj = self._post("/v1/market/history/call-auction", payload, timeout=timeout)
        return decode_rows(obj.get("data"))

    def call_auction_anomaly(self, market: str | None = None) -> list[dict[int, Any]]:
        """集合竞价异动列表(全市场)。

        ⚠️ 实测该接口的响应 schema 未在 OpenAPI 中定义, 字段语义不明:
        字段 10 混着「涨幅档位枚举」(1.0/2.0/3.0…) 与数值, 且出现 +95.86%
        这类超出主板 10% 涨停上限的失真值 (446/802 行为档位枚举)。
        因此 provider **不消费此接口**, 保留仅供人工排查。
        """
        payload: dict[str, Any] = {}
        if market:
            payload["market"] = market
        obj = self._post("/v1/market/history/call-auction-anomaly", payload, timeout=45)
        return decode_rows(obj.get("data"))

    # ---- 涨跌停统计 (语义有官方 schema, 已恒等式验证) ----

    def limit_up_statistics(
        self,
        date: str | None = None,
        filters: tuple[str, ...] = ("HS",),
    ) -> dict[str, Any]:
        """涨跌停统计。支持历史日期回溯 (实测 20260928/29/30 均可用)。

        filters: HS=沪深主板 / GEM2STAR=创业板科创板 / ST=ST 股。
        省略 filters 则不限定板块。

        返回 data 含 date / filters / records; 每条 record:
          direction(limit_up|limit_down) / period(current|previous)
          touched_count 触及数 / sealed_count 收盘封板 / opened_count 盘中打开
          seal_rate 封板率
        实测双恒等式成立: seal_rate == sealed/touched 且 sealed + opened == touched。
        """
        payload: dict[str, Any] = {}
        if date:
            payload["date"] = date.replace("-", "")
        if filters:
            payload["filters"] = list(filters)
        obj = self._post("/v1/market/topics/limit-up-statistics", payload, timeout=45)
        return obj.get("data") or {}

    # ---- 除权除息 / 财务 ----

    def corporate_action(self, symbol: str) -> list[dict[str, Any]]:
        """除权除息记录 → [{date, cash, stock, transfer, rights, rights_price, raw_text}]

        端点 `/v1/market/history/corporate-action` 返回 hxfile 两列:
          1   = 除权日 (YYYYMMDD 整数)
          471 = 可读文本, 形如
                `2026-06-17(每十股 红利0.15元)$`
                `2016-06-07(每十股 送3.00股 转增7.00股 红利0.40元)$`
                `1999-11-29(每十股 配股1.765股 配股价8.00元)$`
                `2005-11-23(  每10股对价股票3.5000股)$`  ← 措辞变体
        所有数值均为**每十股**口径, 此处按原样返回(不做 /10 换算),
        由 provider 统一换算, 避免口径分裂。解析不出的记录 **原样保留 raw_text**
        且数值全为 None, 不猜。
        """
        market, code = symbol_to_market(symbol)
        obj = self._post("/v1/market/history/corporate-action", {"market": market, "code": code}, timeout=30)
        return decode_rows(obj.get("data"))

    def financial(
        self,
        symbol: str,
        fields: list[int],
        count: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[int, Any]]:
        """财务历史记录。

        端点 `/v1/market/history/financial`, **必须显式传 fields**(财务字段编号,
        官方 schema 标为必填, 省略返回 400)。count 与日期范围互斥。
        字段编号的语义未在 OpenAPI 中定义 → 调用方需自行确认含义后使用,
        provider 不做字段名推断。
        """
        market, code = symbol_to_market(symbol)
        payload: dict[str, Any] = {"market": market, "code": code, "fields": list(fields)}
        if start_date and end_date:
            payload["start_date"] = start_date
            payload["end_date"] = end_date
        elif count:
            payload["count"] = int(count)
        obj = self._post("/v1/market/history/financial", payload, timeout=45)
        return decode_rows(obj.get("data"))

    # ---- Level-2 五档盘口 ----

    def level2_depth(self, symbols: list[str]) -> list[dict[int, Any]]:
        """Level-2 五档盘口。

        ⚠️ 实测仅在**交易时段**可用: 休市日(周日)上游返回
        HTTP 400 code=1003「请求内容不完整」, 交易时段外调用会 timeout。
        字段含 5=代码, 24~35=买卖档位价量; 官方 schema 为 Level2DepthSnapshot,
        但 hxfile 形态下的档位字段号未逐一验证, 故 provider 的 depth5
        只映射已验证的核心价量, 档位数组显式置空 —— 不猜字段号。
        """
        secs = []
        for s in symbols:
            m, c = symbol_to_market(s)
            secs.append({"market": m, "code": c})
        obj = self._post("/v1/market/level2/depth", {"securities": secs}, timeout=15)
        return decode_rows(obj.get("data"))


# 市场级熔断: FQGate 某些 market(如北交所 USBJ)上游节点可能整体不通,
# 每次都等满超时会让全量同步被拖成"卡住"(实测 30 秒/只 × 数百只)。
# 一旦某 market 连续 N 次超时, 本进程内直接快速失败, 不再重复等待。
_MARKET_BREAKER_THRESHOLD = 2
_market_fail_count: dict[str, int] = {}
_market_disabled: set[str] = set()


def market_is_disabled(market: str) -> bool:
    return market in _market_disabled


def reset_market_breaker() -> None:
    """清除熔断状态(切换数据源或手动重试时调用)。"""
    _market_fail_count.clear()
    _market_disabled.clear()


def note_market_result(market: str, ok: bool) -> None:
    if ok:
        _market_fail_count[market] = 0
        _market_disabled.discard(market)
        return
    n = _market_fail_count.get(market, 0) + 1
    _market_fail_count[market] = n
    if n >= _MARKET_BREAKER_THRESHOLD:
        if market not in _market_disabled:
            _market_disabled.add(market)
            logger.warning(
                "fqgate 市场 %s 连续 %d 次超时, 本进程内已熔断(快速跳过)。"
                "该市场数据缺失会导致对应标的同步失败。",
                market, n,
            )


_shared: FqgateClient | None = None


def shared_client() -> FqgateClient:
    global _shared
    if _shared is None:
        _shared = FqgateClient()
    return _shared


@contextlib.contextmanager
def probe(timeout: float = 8.0):
    """可用性检测用短生命周期客户端。"""
    c = FqgateClient(timeout=timeout)
    try:
        yield c
    finally:
        with contextlib.suppress(Exception):
            c.close()


def close_shared() -> None:
    global _shared
    if _shared is not None:
        with contextlib.suppress(Exception):
            _shared.close()
        _shared = None
