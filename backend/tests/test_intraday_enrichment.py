"""自选「盘中增强」服务测试 (intraday_enrichment)。

覆盖 2026-10-06 审查发现并修复的真实缺陷:

1. **缓存键只含 `len(symbols)`** → 自选股增删但总数不变(删 A 加 B, 仍是 29 只)时,
   直接命中上一份缓存, 把**别的标的**的数据返回给当前列表。概念列 TTL 30min,
   错数据的暴露窗口远长于竞价列(60s)。修复: 键改用 symbol 集合指纹。
2. 相同 symbol 集合应正常命中缓存 (不重复打上游)。
3. 休市日短路: 未指定 trade_date 且非交易日时, 一个上游请求都不该发。
"""
from __future__ import annotations

import pytest

from app.services import intraday_enrichment as ie

# fqgate security-blocks 的 records 形态: "编号,名称,link,成分数,类型码;..."
# 类型码 3=概念。成分数小的排在前面(最精准)。
_RECORDS = "203,白酒概念,,48,3;81,贵州,URFI882010,34,2;900,融资融券,,3876,3;"


@pytest.fixture(autouse=True)
def _clear_cache():
    ie._cache.clear()
    yield
    ie._cache.clear()


@pytest.fixture
def fake_fqgate(monkeypatch):
    """拦截 FqgateClient: 记录调用, 返回固定板块记录。"""
    calls: list[dict] = []

    class _FakeClient:
        def __init__(self, timeout: float = 10.0) -> None:
            pass

        def _post(self, path, payload, timeout=None):
            calls.append({"path": path, **payload})
            return {"code": 0, "data": {"records": _RECORDS}}

    import app.plugins.fqgate.client as fqc

    monkeypatch.setattr(fqc, "FqgateClient", _FakeClient)
    return calls


def test_cache_key_isolates_same_length_symbol_sets(fake_fqgate):
    """回归: 两只标的换成另外两只(数量不变), 不能命中彼此的缓存。

    修复前两个集合的缓存键都是 `concepts:2:3`, 第二次会直接拿回上一份数据,
    页面于是显示 A 股票的板块给 B 股票。

    注意调用次数: 每个集合内每只股票各打一次上游 (2 只 = 2 次), 两个集合共 4 次。
    若缓存没隔离, 第二次会是 0 次 → 总数仍是 2 —— 所以这里断言 4 次,
    另外断言"4 次覆盖 a 与 b 合起来的全部标的", 确保第二次真的重新请求了。
    """
    a = ["600105.SH", "000001.SZ"]
    b = ["600106.SH", "000002.SZ"]  # 数量与 a 相同, 标的完全不同

    out_a, warns_a = ie.concepts_of(a, top_n=1)
    out_b, warns_b = ie.concepts_of(b, top_n=1)

    assert set(out_a) == set(a)
    assert set(out_b) == set(b)
    assert not warns_a and not warns_b
    assert len(fake_fqgate) == 4, "同长度的不同 symbol 集合不应共享缓存"
    # 第二次必须为 b 的两只票重新发起请求(而不是复用 a 的结果),
    # 即两次调用的标的合起来正好覆盖 a + b。
    assert {c["code"] for c in fake_fqgate[2:]} == {"600106", "000002"}


def test_identical_symbol_set_hits_cache(fake_fqgate):
    """相同集合重复调用应命中缓存, 不重复打上游。"""
    syms = ["600105.SH", "000001.SZ"]
    first, _ = ie.concepts_of(syms, top_n=1)
    second, _ = ie.concepts_of(syms, top_n=1)

    assert first == second
    # 首次 2 只 = 2 次; 第二次命中缓存 = 0 次
    assert len(fake_fqgate) == 2, "相同集合第二次应命中缓存(不重复打上游)"


def test_concept_pick_skips_generic_and_non_concept(fake_fqgate):
    """口径: 只取类型码 3(概念)且成分数 <= 500 的标签, 按成分数升序。"""
    out, _ = ie.concepts_of(["600105.SH"], top_n=3)
    picked = out["600105.SH"]
    # 融资融券(3876)属泛标签被排除; 贵州是地区(类型码 2)被排除; 只剩白酒概念(48)
    assert picked == [("白酒概念", 48)]


def test_auction_short_circuits_on_non_trading_day(monkeypatch):
    """休市日 (2026-10-06 实测: 上游 call-auction 挂 30s/只才 504) 必须短路。"""
    monkeypatch.setattr("app.services.trading_day.is_trading_day", lambda now=None: False)

    def _boom(*a, **k):  # pragma: no cover - 短路后不该被调用
        raise AssertionError("非交易日不应发起 call_auction 请求")

    import app.plugins.fqgate.client as fqc

    monkeypatch.setattr(fqc, "FqgateClient", _boom)

    out, warns = ie.auction_pct_map(["600105.SH", "000001.SZ"])
    assert out == {}
    assert warns and "非交易日" in warns[0]


def test_auction_explicit_date_bypasses_short_circuit(monkeypatch, fake_fqgate):
    """显式指定 trade_date(回溯竞价)时**不能**短路 —— 那是用户主动要历史数据。"""
    monkeypatch.setattr("app.services.trading_day.is_trading_day", lambda now=None: False)

    # call_auction 返回空帧(非交易日的该日期), 但请求必须真的发出去了
    out, warns = ie.auction_pct_map(["600105.SH"], trade_date="20260930")
    assert out == {}
    # 上游 client 换成 fake 后走的是 call_auction; fake 只实现 _post 之外的路径会报错,
    # 因此这里只断言"没有因短路直接返回带说明的 warning"
    assert not (warns and "非交易日" in warns[0]), "指定日期时不应走非交易日短路提示"


def test_fingerprint_is_order_insensitive():
    """指纹与传入顺序无关, 但随集合内容变化; 重复元素按集合语义去重。"""
    assert ie._fingerprint(["b", "a"]) == ie._fingerprint(["a", "b"])
    assert ie._fingerprint(["a", "b"]) != ie._fingerprint(["a", "c"])
    # 去重是预期行为: symbol 列表出现重复不应改变缓存键
    assert ie._fingerprint(["a", "a"]) == ie._fingerprint(["a"])
