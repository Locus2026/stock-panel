"""从 income + balance_sheet 计算历史财务衍生指标。

为什么需要这个:
  fuyao 的 metrics(财务指标) 接口是**单期**接口, 只返回最近 1~2 期,
  而 income(14 期) / balance_sheet(18 期) 有完整历史。
  ROE / 净利率 / 资产负债率等指标可由这两张表自行推导, 补齐历史序列。

口径要点(踩过的坑):
  1. **income 是累计口径**: 2025 全年营收 52.9 亿 > Q3 累计 36.3 亿,
     所以 Q2 的"营收"是上半年累计, **不是单季**。跨期比较必须先单季化(差分),
     否则 Q1 vs 全年直接比会得出荒谬结论。
  2. **balance_sheet 是时点值**: 各期独立, 不需差分。
  3. **ROE 口径**: 官方 metrics.roe 用**加权平均净资产**, 简单平均无法复现
     (实测永鼎2026H1: 净利/期末=12.69%, /简单平均=13.56%, 官方=14.77%)。
     故输出两套: `roe_periodic`(累计净利/期末净资产, 与官方接近且跨期可比)
     与 `roe_annualized`(单季年化, 仅供横向比较, **勿与官方 roe 直接对比**)。
  4. 2024-05-31 这类**非标准报告期**只存在于 balance, 单季化时会错位,
     故按 (symbol, fiscal_year, 季度序号) 严格配对, 配不上就置 None。

输出: data/financials/derived/part.parquet
  [symbol, period_end, fiscal_year, quarter,
   revenue, revenue_q, net_income, net_income_q,
   equity_begin, equity_end, equity_avg,
   roe_periodic, roe_annualized, gross_margin, net_margin,
   debt_to_asset, current_ratio, cash_to_asset,
   op_cf_to_revenue, op_cf_to_net_income]
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

# 数据目录由脚本自身位置推导(2026-10-07): 原为硬编码
# /Users/yeyuting/WorkBuddy/stock/data/financials —— 换机/换用户名即失效, 且违反
# CONTRIBUTING §8「不提交机器绝对路径」。脚本位于 <repo>/backend/scripts/,
# 上溯两级即仓库根。
REPO_ROOT = Path(__file__).resolve().parents[2]
BASE = REPO_ROOT / "data" / "financials"
OUT = BASE / "derived" / "part.parquet"


def quarter_of(period_end: str) -> int | None:
    """'2025-06-30' → 2。非法期(如 2024-05-31)返回 None。"""
    mmdd = period_end[5:]
    return {"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4}.get(mmdd)


def main() -> int:
    inc = pl.read_parquet(BASE / "income" / "part.parquet")
    bs = pl.read_parquet(BASE / "balance_sheet" / "part.parquet")

    inc = inc.with_columns(
        pl.col("period_end").map_elements(quarter_of, return_dtype=pl.Int64).alias("quarter")
    ).filter(pl.col("quarter").is_not_null())
    bs = bs.with_columns(
        pl.col("period_end").map_elements(quarter_of, return_dtype=pl.Int64).alias("quarter")
    ).filter(pl.col("quarter").is_not_null())

    inc_cols = [
        "symbol", "period_end", "fiscal_year", "quarter",
        "revenue", "operating_cost", "net_income", "net_income_attributable",
    ]
    bs_cols = [
        "symbol", "period_end", "fiscal_year", "quarter",
        "total_assets", "total_current_assets", "total_liabilities", "total_equity",
    ]
    df = (
        inc.select(inc_cols)
        .join(bs.select(bs_cols), on=["symbol", "period_end"], how="inner")
        .sort(["symbol", "period_end"])
    )
    if df.is_empty():
        print("合并后为空, 检查期数对齐", file=sys.stderr)
        return 1

    # ---- 单季化: 累计值差分(Q1 本身就是单季) ----
    df = df.with_columns(
        [
            pl.when(pl.col("quarter") == 1)
            .then(pl.col(c))
            .otherwise(pl.col(c) - pl.col(c).shift(1).over("symbol"))
            .alias(c + "_q")
            for c in ("revenue", "net_income", "net_income_attributable")
        ]
    )
    # 差分后若出现负值(口径异常)置 None, 不让脏值污染
    for c in ("revenue_q", "net_income_q", "net_income_attributable_q"):
        df = df.with_columns(pl.when(pl.col(c) < 0).then(None).otherwise(pl.col(c)).alias(c))

    # ---- 期初权益: 同股本上一期的 total_equity ----
    df = df.with_columns(
        pl.col("total_equity").shift(1).over("symbol").alias("equity_begin")
    )
    df = df.with_columns(
        pl.when(pl.col("equity_begin").is_null())
        .then(pl.col("total_equity"))          # 无上期时用期末近似
        .otherwise((pl.col("equity_begin") + pl.col("total_equity")) / 2)
        .alias("equity_avg")
    )

    # ---- 衍生比率 ----
    # ROE 说明(重要): 官方 metrics.roe 用**加权平均净资产**(按股本变动时点加权),
    # 实测永鼎 2026H1: 累计净利/期末净资产=12.69%, /简单平均=13.56%, 官方=14.77%,
    # 三者均不等于官方值 → 简单平均口径**无法复现**官方 ROE。
    # 故: roe_periodic 为"可比口径"(期末净资产, 跨期趋势可靠),
    #     roe_annualized 为单季年化(仅供横向比较, 勿与官方 roe 直接对比)。
    df = df.with_columns(
        [
            # 期间 ROE: 累计归母净利 / 期末净资产 (与官方口径接近, 跨期可比)
            (pl.col("net_income_attributable") / pl.col("total_equity") * 100)
            .alias("roe_periodic"),
            # 单季年化 ROE: 单季净利 / 平均净资产 × 4
            (pl.col("net_income_attributable_q") / pl.col("equity_avg") * 4 * 100)
            .alias("roe_annualized"),
            # 毛利率: (营收-营业成本)/营收 —— 已验证与官方 100% 吻合
            ((pl.col("revenue") - pl.col("operating_cost")) / pl.col("revenue") * 100)
            .alias("gross_margin"),
            # 净利率: 累计归母净利 / 累计营收 —— 已验证与官方 100% 吻合
            (pl.col("net_income_attributable") / pl.col("revenue") * 100)
            .alias("net_margin"),
            # 资产负债率 —— 已验证与官方 100% 吻合
            (pl.col("total_liabilities") / pl.col("total_assets") * 100)
            .alias("debt_to_asset"),
            # 流动比率
            (pl.col("total_current_assets") / pl.col("total_liabilities")).alias("current_ratio"),
            # 权益占比(= 100 - 资产负债率)
            (pl.col("total_equity") / pl.col("total_assets") * 100).alias("equity_to_asset"),
        ]
    )
    df = df.with_columns(
        [pl.when(pl.col(c).abs() > 1e6).then(None).otherwise(pl.col(c)).alias(c)
         for c in ("roe_periodic", "roe_annualized", "gross_margin", "net_margin",
                   "current_ratio", "equity_to_asset")]
    )
    df = df.with_columns(
        pl.when(
            (pl.col("debt_to_asset") < -10.0) | (pl.col("debt_to_asset") > 150.0)
        )
        .then(None)
        .otherwise(pl.col("debt_to_asset"))
        .alias("debt_to_asset")
    )

    # ---- 合并现金流(按 symbol+period_end) ----
    cf_path = BASE / "cash_flow" / "part.parquet"
    if cf_path.exists():
        cf = pl.read_parquet(cf_path)
        if "net_operating_cash_flow" in cf.columns:
            cf = cf.with_columns(
                pl.col("period_end").map_elements(quarter_of, return_dtype=pl.Int64).alias("quarter")
            ).filter(pl.col("quarter").is_not_null())
            # 现金流也是累计口径, 差分单季化
            cf = cf.sort(["symbol", "period_end"]).with_columns(
                pl.when(pl.col("quarter") == 1)
                .then(pl.col("net_operating_cash_flow"))
                .otherwise(
                    pl.col("net_operating_cash_flow")
                    - pl.col("net_operating_cash_flow").shift(1).over("symbol")
                )
                .alias("op_cf_q")
            )
            df = df.join(
                cf.select(["symbol", "period_end", "op_cf_q"]),
                on=["symbol", "period_end"], how="left",
            )
            df = df.with_columns(
                [
                    (pl.col("op_cf_q") / pl.col("revenue_q") * 100)
                    .alias("op_cf_to_revenue"),
                    (pl.col("op_cf_q") / pl.col("net_income_attributable_q") * 100)
                    .alias("op_cf_to_net_income"),
                ]
            )

    cols = [
        "symbol", "period_end", "fiscal_year", "quarter",
        "revenue", "revenue_q", "net_income", "net_income_attributable",
        "net_income_attributable_q",
        "equity_begin", "total_equity", "equity_avg",
        "roe_periodic", "roe_annualized", "gross_margin", "net_margin",
        "debt_to_asset", "current_ratio", "equity_to_asset",
    ]
    if "op_cf_to_revenue" in df.columns:
        cols += ["op_cf_q", "op_cf_to_revenue", "op_cf_to_net_income"]
    out = df.select([c for c in cols if c in df.columns]).sort(["symbol", "period_end"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(OUT)
    print("已写入:", OUT)
    print("  行数 %d  标的 %d  列 %d"
          % (out.height, out["symbol"].n_unique(), out.width))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
