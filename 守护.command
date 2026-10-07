#!/bin/bash
# TSP 后端守护 —— 进程掉线自动拉起
# 用法: ./守护.command          (前台运行, Ctrl+C 退出)
#      ./守护.command --daemon  (后台常驻)

# 路径自推导(2026-10-07): 原为硬编码 /Users/yeyuting/WorkBuddy/stock, 换机/换用户名即失效。
# 从脚本自身位置推导, 项目可整体搬到任意路径。
ROOT="$(cd "$(dirname "$0")" && pwd)"
LOG="/tmp/tsp-backend.log"
WATCH_LOG="/tmp/tsp-guard.log"
PORT=3018
FQGATE=17281

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$WATCH_LOG"; }

fqgate_ready() {
  lsof -nP -tiTCP:$FQGATE -sTCP:LISTEN >/dev/null 2>&1 || return 1
  curl -s --max-time 5 "http://127.0.0.1:$FQGATE/v1/market/health" 2>/dev/null | grep -q '"connected":true'
}

backend_alive() {
  lsof -nP -tiTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1 && \
  curl -s -o /dev/null --max-time 8 "http://127.0.0.1:$PORT/" 2>/dev/null
}

# 清理"僵死"后端进程 (实测 2026-10-04 23:47 的 500/502 根因):
#   uvicorn 进程存活但**已不监听 3018** —— 端口被释放, 而进程里还挂着
#   财务同步的 fuyao 长连接在跑, 事件循环被占死。此时:
#     - backend_alive() 返回 false (端口确实没在听) → 守护判定"掉线"并尝试拉起
#     - 但旧进程未死, 新进程起来后无法绑定端口 → 拉起必然失败 → 死循环
#   故拉起前必须先杀掉所有还活着的 uvicorn 进程(按命令行匹配, 不用端口判断,
#   因为它们已经不监听端口了)。
kill_stale_backends() {
  local pids
  pids=$(pgrep -f "uvicorn app.main:app .*--port $PORT" 2>/dev/null)
  [ -z "$pids" ] && return 0
  local n=0
  for pid in $pids; do
    if kill -9 "$pid" 2>/dev/null; then n=$((n + 1)); fi
  done
  if [ "$n" -gt 0 ]; then
    log "   已清理 $n 个僵死后端进程(存活但不监听端口)"
    sleep 2
  fi
}

start_backend() {
  cd "$ROOT/backend" || return 1
  # 拉起前先清掉僵死进程(见 kill_stale_backends 的注释), 否则新进程绑不了端口
  kill_stale_backends
  # 预清理 numba 缓存临时文件:
  # backtest.matrix 用 numba JIT, 每次启动会在 __pycache__ 下留 tmpXXXX 临时文件。
  # 累积到 50 个时触发环境的批量删除保护(SAFE_DELETE_BULK_CONFIRM_REQUIRED),
  # tempfile 清理抛异常 → 启动中断 → 守护陷入"拉起失败→重试→再失败"死循环。
  # 这里用 mv 移到废纸篓(不删除), 符合小批次原则。
  local cache_dir="$ROOT/backend/app/backtest/__pycache__"
  if [ -d "$cache_dir" ]; then
    local n=0
    for f in "$cache_dir"/tmp*; do
      [ -f "$f" ] || continue
      mv "$f" "$HOME/.Trash/tsp-pycache-$(basename "$f")" 2>/dev/null && n=$((n + 1))
    done
    [ "$n" -gt 0 ] && log "   已清理 $n 个 numba 临时缓存文件"
  fi
  # 双 fork 脱离终端会话
  # ROOT 经环境变量传入: heredoc 是 <<'PYEOF' (不展开 shell 变量), 在 Python 里
  # 写死绝对路径会让脚本绑死在某台机器上 (2026-10-07 改为路径自推导)。
  TSP_BACKEND_DIR="$ROOT/backend" .venv/bin/python - <<'PYEOF' >> /tmp/tsp-guard-stdout.log 2>&1
import os, sys
if os.fork() > 0:
    sys.exit(0)
os.setsid()
if os.fork() > 0:
    os._exit(0)
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
  # 等待就绪(最多 90 秒, 后端启动约 20-30 秒)
  for i in $(seq 1 45); do
    sleep 2
    if lsof -nP -tiTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
      for j in $(seq 1 20); do
        grep -q "Application startup complete" "$LOG" 2>/dev/null && break
        sleep 1
      done
      return 0
    fi
  done
  return 1
}

log "=== TSP 后端守护启动 ==="
log "根目录: $ROOT"

# 首次检查
if backend_alive; then
  log "✅ 后端已在运行 (PID $(lsof -nP -tiTCP:$PORT -sTCP:LISTEN | head -1))"
else
  log "▶️ 后端未运行, 首次拉起..."
  if start_backend; then
    log "✅ 后端启动成功 (PID $(lsof -nP -tiTCP:$PORT -sTCP:LISTEN | head -1))"
  else
    log "❌ 首次启动失败, 将继续守护重试"
  fi
fi

# 守护循环
FAIL=0
while true; do
  sleep 10
  if backend_alive; then
    if [ "$FAIL" -gt 0 ]; then
      log "✅ 后端已恢复正常"
      FAIL=0
    fi
    continue
  fi

  FAIL=$((FAIL + 1))
  log "⚠️  检测到后端掉线 (第 ${FAIL} 次)"

  # FQGate 未就绪时先拉 FQGate
  if ! fqgate_ready; then
    if [ -d "/Applications/FQGate.app" ]; then
      log "▶️ FQGate 节点未连接, 尝试拉起..."
      open -a "/Applications/FQGate.app"
      for i in $(seq 1 15); do
        sleep 2
        fqgate_ready && { log "✅ FQGate 就绪"; break; }
      done
    fi
  fi

  log "▶️ 重新拉起后端..."
  if start_backend; then
    log "✅ 后端已重新启动 (PID $(lsof -nP -tiTCP:$PORT -sTCP:LISTEN | head -1))"
    FAIL=0
    # 数据源确认
    sleep 2
    R=$(curl -s --max-time 20 -X PUT -H "Content-Type: application/json" \
      -d '{"daily_data_provider":"fqgate","realtime_data_provider":"fqgate","minute_data_provider":"fqgate","adj_factor_provider":"fqgate","depth5_data_provider":"fqgate"}' \
      "http://127.0.0.1:$PORT/api/settings/preferences/data-providers" 2>/dev/null)
    case "$R" in
      *fqgate*) log "✅ 数据源已确认 fqgate" ;;
      *)        log "⚠️ 数据源切换未生效, 可在页面设置-数据源手动选择" ;;
    esac
  else
    log "❌ 拉起失败 (第 ${FAIL} 次)"
    # 连续失败达上限则停手并留下诊断线索, 避免无限重试刷屏掩盖真因。
    if [ "$FAIL" -ge 5 ]; then
      log "🛑 已连续 ${FAIL} 次拉起失败, 暂停自动重试(60 秒后再试)"
      log "   诊断: tail -30 /tmp/tsp-backend.log"
      if grep -q "SAFE_DELETE_BULK_CONFIRM_REQUIRED" "$LOG" 2>/dev/null; then
        log "   疑似批量删除保护触发, 可执行: ./清理缓存.command"
      fi
      for i in $(seq 1 6); do
        sleep 10
        backend_alive && break
      done
      FAIL=0
      log "   继续守护"
    else
      log "   10 秒后重试"
    fi
  fi
done
