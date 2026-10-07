#!/bin/bash
# 查看 TSP 各服务状态
# 路径自推导(2026-10-07): 原为硬编码 /Users/yeyuting/WorkBuddy/stock, 换机即失效。
# 现在从脚本自身位置推导, 项目可以整个搬到任意路径/任意用户名。
ROOT="$(cd "$(dirname "$0")" && pwd)"

# JSON 解析用的解释器(2026-10-07): 原写死 /Users/yeyuting/.workbuddy/binaries/
# python/versions/3.13.12/bin/python3 —— 既绑用户名又绑 WorkBuddy 版本号, 换机
# 或 WorkBuddy 升级后必然失效。改为三级探测, 都没有就跳过 JSON 解析(只影响
# 两处"登录方式/数据源"的可读性展示, 不影响本脚本的服务状态判定)。
pick_python() {
  if [ -x "$ROOT/backend/.venv/bin/python" ]; then echo "$ROOT/backend/.venv/bin/python"; return; fi
  for c in python3 /usr/bin/python3 python; do
    if command -v "$c" >/dev/null 2>&1; then echo "$c"; return; fi
  done
  echo ""
}
PY_BIN="$(pick_python)"

line() { printf "  %-14s " "$1"; }

echo "=============================================="
echo "  TSP 量化工作台 · 状态"
echo "=============================================="
echo ""

# FQGate
line "FQGate(17281)"
if lsof -nP -tiTCP:17281 -sTCP:LISTEN >/dev/null 2>&1; then
  PID=$(lsof -nP -tiTCP:17281 -sTCP:LISTEN | head -1)
  H=$(curl -s --max-time 5 http://127.0.0.1:17281/v1/market/health 2>/dev/null)
  if echo "$H" | grep -q '"connected":true'; then
    if [ -n "$PY_BIN" ]; then
      LOGIN=$(echo "$H" | "$PY_BIN" -c "import sys,json;print(json.load(sys.stdin)['data'].get('login_method',''))" 2>/dev/null)
    fi
    echo "✅ 运行中 (PID $PID) ${LOGIN:+登录=$LOGIN}"
  else
    echo "⚠️  运行中 (PID $PID) 但行情节点未连接"
  fi
else
  echo "❌ 未运行"
fi

# 后端
echo ""
line "后端(3018)"
if lsof -nP -tiTCP:3018 -sTCP:LISTEN >/dev/null 2>&1; then
  PID=$(lsof -nP -tiTCP:3018 -sTCP:LISTEN | head -1)
  UPTIME=$(ps -p "$PID" -o etime= 2>/dev/null | tr -d " ")
  [ -z "$UPTIME" ] && UPTIME="刚启动"
  CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 8 http://127.0.0.1:3018/ 2>/dev/null)
  echo "✅ 运行中 (PID $PID) 已运行 $UPTIME  HTTP $CODE"
else
  echo "❌ 未运行"
fi

# 前端(可选)
echo ""
line "前端(3011)"
if lsof -nP -tiTCP:3011 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "✅ 运行中 (开发模式)"
else
  echo "· 未运行（正常，后端已托管前端产物）"
fi

# 守护
echo ""
line "守护进程"
if pgrep -f "守护.command" >/dev/null 2>&1; then
  echo "✅ 运行中 — 后端掉线会自动拉起"
else
  echo "❌ 未运行 — 执行 ./启动.command 或 ./守护.command 开启"
fi

# 数据源
echo ""
line "数据源"
DS=$(curl -s --max-time 10 -X PUT -H "Content-Type: application/json" \
  -d '{}' http://127.0.0.1:3018/api/settings/preferences/data-providers 2>/dev/null)
if [ -n "$DS" ] && [ -n "$PY_BIN" ]; then
  echo "$DS" | "$PY_BIN" -c "
import sys,json
try:
    d=json.load(sys.stdin)
    for k,v in sorted(d.items()):
        mark='✅' if v=='fqgate' else '↩︎'
        print(f'{mark} {k.replace(\"_provider\",\"\").replace(\"_data\",\"\"):16} = {v}')
except Exception:
    pass
" 2>/dev/null
elif [ -n "$DS" ]; then
  echo "· 未找到可用的 python3, 跳过数据源解析"
else
  echo "· 后端未运行，无法查询"
fi

echo ""
echo "=============================================="
echo "  网页 http://127.0.0.1:3018"
echo "  日志 /tmp/tsp-backend.log  (后端)"
echo "        /tmp/tsp-guard.log    (守护)"
echo "=============================================="
