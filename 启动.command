#!/bin/bash
# TSP A股量化工作台 一键启动
# 用法: 在终端执行  ./启动.command
# 或在访达里双击「启动.command」

cd "$(dirname "$0")" || exit 1
# 项目根目录 (2026-10-07): 供下方 heredoc 内的 Python 用, 避免写死机器绝对路径。
ROOT="$PWD"

echo "=============================================="
echo "  TSP A股量化工作台 · 启动中"
echo "=============================================="
echo ""

# 1. 检查并自动拉起 FQGate（同花顺数据源）
fqgate_node_ready() {
  # 真正就绪的判据: 端口在监听 **且** 行情节点已连接
  lsof -nP -tiTCP:17281 -sTCP:LISTEN >/dev/null 2>&1 || return 1
  curl -s --max-time 5 http://127.0.0.1:17281/v1/market/health 2>/dev/null \
    | grep -q '"connected":true'
}

if fqgate_node_ready; then
  echo "✅ FQGate 数据源已就绪"
else
  if ! lsof -nP -tiTCP:17281 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "▶️  FQGate 未运行，正在启动..."
    if [ -d "/Applications/FQGate.app" ]; then
      open -a "/Applications/FQGate.app"
    else
      echo "⚠️  未找到 /Applications/FQGate.app，请手动打开 FQGate 主程序"
    fi
  fi

  printf "   等待行情节点连接"
  for i in $(seq 1 30); do
    sleep 2
    if fqgate_node_ready; then
      echo " ✅"
      break
    fi
    printf "."
  done
  echo ""
  if fqgate_node_ready; then
    echo "✅ FQGate 就绪"
  else
    echo ""
    echo "⚠️  FQGate 已启动但行情节点未连接"
    echo "   → 请切到 FQGate 窗口，在里面登录同花顺账号（游客也可，只是数据有延迟）"
    echo "   → 登录后重新执行本脚本，或直接刷新 http://127.0.0.1:3018"
  fi
fi
echo ""

# 2. 检查后端端口
if lsof -nP -tiTCP:3018 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "✅ 后端已在运行"
else
  echo "▶️  启动后端 (端口 3018)..."
  cd backend || exit 1

  # 预清理 numba 临时缓存:
  # app/backtest/matrix.py 用 numba JIT, 每次启动会在 __pycache__ 留 tmpXXXX 文件。
  # 累积到 50 个触发批量删除保护 → tempfile 清理抛异常 → 启动中断。
  # 这里用 mv 移到废纸篓(不删除), 符合小批次原则。
  CACHE_DIR="app/backtest/__pycache__"
  if [ -d "$CACHE_DIR" ]; then
    CLEANED=0
    for f in "$CACHE_DIR"/tmp*; do
      [ -f "$f" ] || continue
      mv "$f" "$HOME/.Trash/tsp-pycache-$(basename "$f")" 2>/dev/null && CLEANED=$((CLEANED + 1))
    done
    [ "$CLEANED" -gt 0 ] && echo "   已清理 $CLEANED 个 numba 临时缓存"
  fi

  # 预清理僵死后端进程 (实测 2026-10-04 23:47 的 500/502 根因):
  # uvicorn 可能"进程存活但已不监听 3018"(端口被释放, 进程里还挂着财务同步的
  # fuyao 长连接占死事件循环)。此时端口检查判定为"未运行", 但旧进程仍在,
  # 新进程无法绑定端口 → 启动必然失败。按命令行匹配杀, 不能只看端口。
  STALE=$(pgrep -f "uvicorn app.main:app .*--port 3018" 2>/dev/null)
  if [ -n "$STALE" ]; then
    KILLED=0
    for pid in $STALE; do
      kill -9 "$pid" 2>/dev/null && KILLED=$((KILLED + 1))
    done
    [ "$KILLED" -gt 0 ] && echo "   已清理 $KILLED 个僵死后端进程"
    sleep 2
  fi

  # macOS 无 setsid, 用 Python 双 fork 彻底脱离终端会话:
  # 关闭终端 / 脚本退出后后端继续运行。
  # ROOT 通过环境变量传入: heredoc 用 <<'PYEOF' (引号版) 不展开 shell 变量,
  # 直接在 Python 里写路径就又变成机器绝对路径了 (2026-10-07 起改为路径自推导)。
  TSP_BACKEND_DIR="$ROOT/backend" .venv/bin/python - <<'PYEOF' > /tmp/tsp-backend.log 2>&1
import os, sys
if os.fork() > 0:
    sys.exit(0)                      # 父进程立刻返回
os.setsid()                           # 新会话, 脱离控制终端
if os.fork() > 0:
    os._exit(0)                       # 中间进程退出
# 孙进程: 重定向 std 三件套, 彻底脱离
fd = os.open("/dev/null", os.O_RDONLY)
os.dup2(fd, 0)
log = os.open("/tmp/tsp-backend.log", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.dup2(log, 1); os.dup2(log, 2)
os.chdir(os.environ["TSP_BACKEND_DIR"])
os.execv(".venv/bin/python", [
    ".venv/bin/python", "-m", "uvicorn", "app.main:app",
    "--env-file", "../.env", "--host", "127.0.0.1", "--port", "3018",
])
PYEOF
  cd ..

  printf "   等待后端就绪"
  READY=0
  for i in $(seq 1 30); do
    sleep 2
    if lsof -nP -tiTCP:3018 -sTCP:LISTEN >/dev/null 2>&1; then
      READY=1
      break
    fi
    printf "."
  done
  echo ""

  if [ "$READY" = "1" ]; then
    for i in $(seq 1 20); do
      if grep -q "Application startup complete" /tmp/tsp-backend.log 2>/dev/null; then
        break
      fi
      sleep 1
    done
    echo "✅ 后端启动成功"
  else
    echo "❌ 后端启动失败，日志: /tmp/tsp-backend.log"
    tail -5 /tmp/tsp-backend.log
    exit 1
  fi
fi
echo ""

# 3. 打开浏览器
echo ""
echo "▶️  打开浏览器..."
open "http://127.0.0.1:3018"
echo ""

# 后端启动时若 FQGate 尚未连上, 插件会被判不可用并回退 tickflow;
# 此处再确认一次, 连上则自动切回 fqgate。
if [ "$READY" = "1" ] && fqgate_node_ready; then
  sleep 2
  SWITCH=$(curl -s --max-time 20 -X PUT \
    -H "Content-Type: application/json" \
    -d '{"daily_data_provider":"fqgate","realtime_data_provider":"fqgate"}' \
    http://127.0.0.1:3018/api/settings/preferences/data-providers 2>/dev/null)
  case "$SWITCH" in
    *fqgate*) echo "✅ 数据源已确认使用 fqgate" ;;
    *)        echo "⚠️  数据源切换未生效，可在页面「设置 → 数据源」手动选择" ;;
  esac
fi
echo ""
echo "=============================================="
echo "  ✅ 全部就绪"
echo ""
echo "  网页界面  http://127.0.0.1:3018"
echo "  守护进程  已启用（掉线会自动拉起）"
echo "  查看状态  执行 ./状态.command"
echo "  停止服务  执行 ./停止.command"
echo "=============================================="

# 4. 启动守护（后端已在上面起来了，这里只负责常驻监控）
if [ "$READY" = "1" ]; then
  # 守护已在跑就不重复起, 否则拉起一个常驻的
  if pgrep -f "守护.command" >/dev/null 2>&1; then
    echo ""
    echo "🛡️  守护已在运行 — 后端掉线会自动拉起"
  else
    setsid bash "$(dirname "$0")/守护.command" >/dev/null 2>&1 </dev/null &
    disown 2>/dev/null
    # 给守护 3 秒完成自身初始化与首检
    sleep 3
    if pgrep -f "守护.command" >/dev/null 2>&1; then
      echo ""
      echo "🛡️  守护已启动 — 后端掉线会自动拉起（每 10 秒检测）"
      echo "     状态查看: ./状态.command"
      echo "     守护日志: /tmp/tsp-guard.log"
    else
      echo ""
      echo "⚠️  守护启动失败，可手动前台执行: ./守护.command"
    fi
  fi
fi
