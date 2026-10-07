#!/bin/bash
# 清理 numba / Python 临时缓存
#
# 为什么需要:
#   backend/app/backtest/matrix.py 用 numba JIT 编译, 每次启动会在
#   __pycache__ 下留一批 tmpXXXX 临时文件。累积到 50 个时触发环境的
#   批量删除保护(SAFE_DELETE_BULK_CONFIRM_REQUIRED), tempfile 清理抛异常
#   → 后端启动中断 → 守护反复"拉起失败"。
#
# 本脚本用 mv 移到废纸篓(不删除), 符合小批次原则。

# 路径自推导(2026-10-07): 原为硬编码 /Users/yeyuting/WorkBuddy/stock
ROOT="$(cd "$(dirname "$0")" && pwd)"
STAMP=$(date +%H%M%S)
N=0

echo "=== 清理临时缓存 ==="

# 1) numba tmp 文件(主要来源)
for d in "$ROOT"/backend/app/*/__pycache__ "$ROOT"/backend/app/*/*/__pycache__; do
  [ -d "$d" ] || continue
  for f in "$d"/tmp*; do
    [ -f "$f" ] || continue
    mv "$f" "$HOME/.Trash/tsp-cache-$STAMP-$(basename "$f")" 2>/dev/null && N=$((N + 1))
  done
done
echo "  numba 临时文件: $N 个"

# 2) 统计 __pycache__ 现状(阈值 50)
TOTAL=$(find "$ROOT/backend/app" -type f -path "*/__pycache__/*" 2>/dev/null | wc -l | tr -d ' ')
echo "  __pycache__ 剩余文件: $TOTAL 个 (阈值 50)"

if [ "$TOTAL" -lt 50 ]; then
  echo "  ✅ 低于阈值, 启动不会再触发保护"
else
  echo "  ⚠️  仍高于阈值, 建议重启后端让 numba 重新生成缓存"
  echo "     正常 numba 缓存文件(matrix._valid_*kernel-*.nbc)无需清理"
fi

echo ""
echo "已移到废纸篓(前缀 tsp-cache-$STAMP-), 需要时可从废纸篓恢复"
echo "接下来执行 ./启动.command 或等守护自动拉起"
