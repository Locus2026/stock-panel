# 新机部署清单（从 GitHub 克隆到可用）

面向**新电脑首次部署**。核心前提：GitHub 只带代码，`data/`（历史行情，约 240MB）
与 `.env`（密钥）都在 `.gitignore` 里，必须在新机重新获取——本清单给出完整顺序。

---

## 0. 前置条件

| 依赖 | 版本 | 说明 |
|---|---|---|
| macOS | — | FQGate 是 macOS app；`.command` 脚本依赖 zsh/bash 与 `lsof` |
| Python | ≥ 3.11 | `backend/pyproject.toml` 目标 py311；推荐用 `uv` 管理 |
| uv | 最新 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Node.js | ≥ 20 | 前端构建（`node -v`） |
| **FQGate.app** | — | **本机同花顺行情源，不在仓库里**，见第 3 步 |

FQGate 需要登录同花顺账号，它的授权**不随项目迁移**——这是新机最常见的第一个坑。

---

## 1. 克隆与依赖

```bash
git clone https://github.com/shy3130/tick-stock-panel.git ~/stock
cd ~/stock

# 后端：按 uv.lock 精确还原依赖（不要用 pip install -r，项目没有 requirements.txt）
cd backend && uv sync && cd ..

# 前端：npm ci 用 lock 文件严格对齐版本
cd frontend && npm ci && cd ..
```

依赖目录（`.venv` / `node_modules`）**不在仓库里**，必须重建，这是正常的。

---

## 2. 配置 `.env`

仓库里的 `.env.example` 是模板，**不能直接用**（不含真实密钥）。

```bash
cp .env.example .env
chmod 600 .env
```

至少需要填这几项（其余保持默认）：

| 键 | 说明 |
|---|---|
| `FQGATE_BASE_URL` | 本机 FQGate 地址，默认 `http://127.0.0.1:17281` |
| `TICKFLOW_API_KEY` | fqgate 未覆盖的数据集（财务等）回退 tickflow 时需要 |
| `AI_API_KEY` / `AI_BASE_URL` / `AI_MODEL` | AI 策略功能；不用可留空 |

> `.env` 权限保持 600，不要提交进 Git（`.gitignore` 已忽略）。

---

## 3. 安装并登录 FQGate

```bash
open -a /Applications/FQGate.app      # 若已安装
```

登录同花顺账号后**必须确认已连接**，否则行情相关功能全部空：

```bash
curl -s http://127.0.0.1:17281/v1/market/health | grep -o '"connected":[a-z]*'
# 期望输出: "connected":true
```

`./启动.command` 也会自动尝试拉起 FQGate 并等待就绪。

---

## 4. 启动

```bash
./启动.command          # 首次启动, 会拉起 FQGate + 后端并打开浏览器
./状态.command          # 随时查看 FQGate/后端/前端/守护/数据源状态
./守护.command --daemon # 需要开机自启/掉线自动重拉时
./停止.command          # 停止
```

后端默认在 <http://127.0.0.1:3018>，日志在 `/tmp/tsp-backend.log`、`/tmp/tsp-guard.log`。

脚本已做**路径无关**处理（`ROOT="$(cd "$(dirname "$0")" && pwd)"`），
放在任何路径、任何用户名下都能运行，无需修改。

---

## 5. 拉数据（`data/` 是空的，必须重爬）

**顺序有依赖**：标的列表是所有数据的前提，概念/行业是展示层的前提。

推荐走项目自带的 **Data 页**（`/data`）与管线任务，而非手敲 curl——接口参数与
并发控制都在后端兜着，直接 curl 容易漏参数或打爆上游。

按这个顺序推进：

| 顺序 | 数据 | 用途 | 缺失时的表现 |
|---|---|---|---|
| 1 | **标的列表 / 交易日历** | 全市场代码表 | 所有选股、行情都为空 |
| 2 | **日线 K** | 复权价、涨停判定、绝大多数策略 | K 线图空白、连板天梯无梯队 |
| 3 | **概念 / 行业**（扩展数据预设） | 连板天梯的题材标签、概念热度卡 | 天梯不显示题材、热度卡空白 |
| 4 | **指数 / ETF 日线** | 看板的涨跌对比 | 看板指数区空白 |
| 5 | **财务数据** | 基本面筛选、财务派生指标 | 财务相关策略不可用 |
| 6 | **分钟 K**（最慢，可最后做） | 竞价/盘中形态、实时监控 | 日线类功能正常，分钟策略不可用 |

数据量大时优先在盘后跑（上游限流与时段相关）。日线与财务走 `sync_batch` /
批量接口，不要逐只循环调用。

---

## 6. 验证清单

全部通过才算部署完成：

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:3018/api/strategies   # 期望 200
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:3018/api/watchlist     # 期望 200
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:3018/api/data/status    # 期望 200, 看数据日期
```

页面侧逐项确认：

1. 看板：指数/KPI/涨跌榜有数据，概念热度与行业热度**领涨股名完整显示**
2. 选股页：策略卡片墙出现（内置 25 个 + 你创建的叠加策略），点「刷新」能跑出结果
3. 连板天梯：各梯队有股票，**每只股票显示题材标签且按梯内热度排序**
4. 自选页：竞价列与概念列有内容（**休市日竞价列显示 "—" 属正常**）

---

## 7. 常见故障

| 现象 | 原因 | 处理 |
|---|---|---|
| `502 upstream connect failed` | 后端没起或已崩 | `./状态.command` 看端口；`./守护.command --daemon` 常驻 |
| 行情全空、K 线空白 | FQGate 未连接 | 见第 3 步；`/v1/market/health` 须 `connected:true` |
| 竞价列一直是 "—" | 非交易日或未到 09:25 | 正常；服务端会在 `warnings` 里说明 |
| 请求超时（30s） | 上游行情源抖动 | 盘中增强类接口已有 12s 硬预算，超时会降级并在 `warnings` 自披露 |
| 策略卡片不显示新建的 | 浏览器 localStorage 里的策略池是旧的 | 策略池对话框里手动加回一次；之后不会再丢 |
| 天梯不显示题材标签 | 概念扩展数据未拉取 | 补第 5 步第 3 项 |
| 后端启动报 numba 临时文件相关失败 | `__pycache__` 累积触发批量删除保护 | `./清理缓存.command`（移到废纸篓，不删除） |

日志入口：`/tmp/tsp-backend.log`（后端）、`/tmp/tsp-guard.log`（守护）。

---

## 8. 之后想把老机器的数据搬过来

`data/` 约 240MB，不走 Git。要省掉重爬的时间，直接 rsync 过去最省事：

```bash
# 在老机器执行
rsync -av --progress --exclude 'backend.log*' \
  /Users/<老用户名>/WorkBuddy/stock/data/ 新机用户名@新机IP:~/stock/data/
```

注意：**先在新机 `./停止.command`**，运行中拷贝可能拿到半写状态的 parquet。
搬完再启动，然后跑一遍第 6 步的验证清单确认数据连续。