#!/bin/bash
# 停止 TSP A股量化工作台
# 用法: 执行 ./停止.command

cd "$(dirname "$0")" || exit 1

echo "正在停止服务..."

# 停后端
P=$(lsof -nP -tiTCP:3018 -sTCP:LISTEN 2>/dev/null)
if [ -n "$P" ]; then
  kill $P 2>/dev/null
  echo "✅ 后端已停止 (PID $P)"
else
  echo "· 后端未在运行"
fi

# 停前端开发服务器
P=$(lsof -nP -tiTCP:3011 -sTCP:LISTEN 2>/dev/null)
if [ -n "$P" ]; then
  kill $P 2>/dev/null
  echo "✅ 前端已停止 (PID $P)"
else
  echo "· 前端未在运行"
fi

echo ""
echo "提示: FQGate 主程序未被停止（如有需要请手动退出）"
echo "数据不会丢，下次启动 ./启动.command 即可继续"
