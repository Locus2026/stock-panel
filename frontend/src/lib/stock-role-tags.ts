/**
 * 个股角色标注 (龙头 / 中军 / 跟风 / 补涨 / 核心) —— **纯数据层**。
 *
 * 纯用户手工标记 —— 不参与任何选股/策略计算, 只做盘后复盘时的角色归档,
 * 因此**不上后端**, 只落 localStorage (与 watchlistColumns 同层级)。
 *
 * 口径说明 (重要, 避免误读):
 *   - 一个标的**只能有一个角色** (单选)。可覆盖修改。
 *   - 标记带日期 (标记当天), 便于按交易日回看"当日谁是龙头"。
 *   - 清除: 对同一标的再次点击已选角色 = 取消标注。
 *
 * React 组件 (RoleTagCell) 见 components/stock-table/RoleTagCell.tsx。
 */

export const ROLE_TAGS = [
  { key: 'core', label: '核心', color: 'text-amber-400 bg-amber-500/12 border-amber-500/30' },
  { key: 'leader', label: '龙头', color: 'text-danger bg-danger/12 border-danger/30' },
  { key: 'mid', label: '中军', color: 'text-accent bg-accent/12 border-accent/30' },
  { key: 'follower', label: '跟风', color: 'text-secondary bg-elevated border-border' },
  { key: 'catchup', label: '补涨', color: 'text-bull bg-bull/12 border-bull/30' },
] as const

export type RoleKey = (typeof ROLE_TAGS)[number]['key']

export interface RoleMark {
  role: RoleKey
  /** 标记日期 YYYY-MM-DD */
  date: string
  /** 标记时间戳(ms), 便于同日内按时间排序 */
  ts: number
}

const STORAGE_KEY = 'tsp_stock_role_marks'

function todayStr(): string {
  const d = new Date()
  const p = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`
}

function readAll(): Record<string, RoleMark> {
  if (typeof localStorage === 'undefined') return {}
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return {}
    const obj = JSON.parse(raw)
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return {}
    // 逐条校验, 丢弃非法项(避免手改 localStorage 造成渲染崩溃)
    const out: Record<string, RoleMark> = {}
    for (const [sym, v] of Object.entries(obj as Record<string, unknown>)) {
      if (!v || typeof v !== 'object') continue
      const m = v as Partial<RoleMark>
      if (typeof m.role !== 'string') continue
      if (!ROLE_TAGS.some(t => t.key === m.role)) continue
      out[sym] = {
        role: m.role as RoleKey,
        date: typeof m.date === 'string' ? m.date : '',
        ts: typeof m.ts === 'number' && Number.isFinite(m.ts) ? m.ts : 0,
      }
    }
    return out
  } catch {
    return {}
  }
}

function writeAll(marks: Record<string, RoleMark>) {
  if (typeof localStorage === 'undefined') return
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(marks))
    // 同标签页内广播, 让其他 useRoleMarks 实例同步刷新
    window.dispatchEvent(new CustomEvent('tsp-role-marks-changed'))
  } catch {
    // 配额满/隐私模式: 静默失败, 界面仍可读当前内存态
  }
}

/** 读出当前全部标注 (供 hook 初次挂载与变更时调用)。 */
export function readAllRoleMarks(): Record<string, RoleMark> {
  return readAll()
}

/**
 * 切换标注: 已选同一角色 → 取消; 否则设为该角色 (覆盖旧角色)。
 * 返回操作后的新标注 (取消时为 null)。
 */
export function toggleRoleMark(symbol: string, role: RoleKey): RoleMark | null {
  const all = readAll()
  const cur = all[symbol]
  if (cur && cur.role === role) {
    delete all[symbol]
    writeAll(all)
    return null
  }
  const next: RoleMark = { role, date: todayStr(), ts: Date.now() }
  all[symbol] = next
  writeAll(all)
  return next
}

/** 清除某标的标注。 */
export function clearRoleMark(symbol: string) {
  const all = readAll()
  if (all[symbol]) {
    delete all[symbol]
    writeAll(all)
  }
}

/** 取某标的的标注 (无则 null)。 */
export function getRoleMark(symbol: string): RoleMark | null {
  return readAll()[symbol] ?? null
}

export function roleMeta(key: string) {
  return ROLE_TAGS.find(t => t.key === key) ?? null
}