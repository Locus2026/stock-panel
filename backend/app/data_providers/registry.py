"""Provider registry —— 内置数据源注册表。

2026-10-04 起 **fqgate(本机 FQGate / 同花顺) 为默认内置源**, tickflow 降级为回退源。

两者都保留的原因:
  - fqgate 覆盖 日K / 全市场实时快照 / 五档盘口, 但**不提供** 分钟K / 除权因子 /
    财务数据; 这些数据集仍需回退 tickflow, 否则复权价与涨停判定会断供。
  - fqgate 不可用时(未启动 / 未登录) 可整体回退 tickflow, 保证服务不中断。

插件类数据源(fuyao / stocksdk / fqgate-plugin 等)不在此表, 由
app.data_providers.custom.loader 单独注册; 两者由 services 层统一路由。
"""
from __future__ import annotations

from app.data_providers.tickflow_provider import TickFlowProvider

# 内置源: name → Provider 类
_PROVIDERS = {
    "tickflow": TickFlowProvider,
}

# 默认数据源(与 app.services.preferences._DEFAULT_DATA_PROVIDER 保持一致)
DEFAULT_PROVIDER = "fqgate"
# 回退数据源(fqgate 未声明的数据集走这里)
FALLBACK_PROVIDER = "tickflow"


def get_provider(name: str = ""):
    """按名字取内置 provider。

    name 为空时返回回退源(tickflow) —— 内置表里没有 fqgate,
    fqgate 作为插件由 custom loader 解析; services 层会先查 custom 表,
    查不到才落到这里, 因此这里给回退源而非默认源。
    """
    key = (name or FALLBACK_PROVIDER).lower()
    provider_cls = _PROVIDERS.get(key)
    if provider_cls is None:
        raise ValueError(f"Unsupported data provider: {name}")
    return provider_cls()
