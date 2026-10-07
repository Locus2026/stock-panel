/**
 * 个股角色标注单元格 (龙头 / 中军 / 跟风 / 补涨 / 核心)。
 *
 * 交互: 点击标签 → 弹出角色菜单; 选已选中的角色 = 取消标注。
 * 状态落 localStorage (见 lib/stock-role-tags), 不上后端。
 */
import { useEffect, useRef, useState } from 'react'
import {
  ROLE_TAGS, readAllRoleMarks, toggleRoleMark, roleMeta,
  type RoleKey, type RoleMark,
} from '@/lib/stock-role-tags'

/** 订阅全部标注; 同标签页内任一处修改会同步刷新。 */
export function useRoleMarks(): Record<string, RoleMark> {
  const [marks, setMarks] = useState<Record<string, RoleMark>>({})

  useEffect(() => {
    setMarks(readAllRoleMarks())
    const onChange = () => setMarks(readAllRoleMarks())
    window.addEventListener('storage', onChange)
    window.addEventListener('tsp-role-marks-changed', onChange)
    return () => {
      window.removeEventListener('storage', onChange)
      window.removeEventListener('tsp-role-marks-changed', onChange)
    }
  }, [])

  return marks
}

/**
 * 角色标注单元格。
 * @param symbol 标的代码, localStorage 键
 * @param marks  由 useRoleMarks() 提供的标注快照
 */
export function RoleTagCell({ symbol, marks }: { symbol: string; marks: Record<string, RoleMark> }) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const cur = marks[symbol]
  const meta = cur ? roleMeta(cur.role) : null

  // 点击外部 / Esc 关闭
  useEffect(() => {
    if (!open) return
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const pick = (k: RoleKey) => {
    toggleRoleMark(symbol, k)
    setOpen(false)
  }

  return (
    <div ref={ref} className="relative inline-block">
      <button
        type="button"
        onClick={e => { e.stopPropagation(); setOpen(v => !v) }}
        title={meta ? `角色: ${meta.label} (${cur?.date}) — 点击修改` : '点击标注角色'}
        className={`inline-block px-1.5 py-px rounded text-[10px] font-medium leading-tight border whitespace-nowrap transition-colors cursor-pointer ${
          meta ? meta.color : 'text-muted border-border hover:border-accent/50 hover:text-accent'
        }`}
      >
        {meta ? meta.label : '+标注'}
      </button>

      {open && (
        <div
          onClick={e => e.stopPropagation()}
          className="absolute left-0 top-full mt-1 z-50 min-w-[104px] rounded-card border border-border bg-surface shadow-xl py-1"
        >
          {ROLE_TAGS.map(t => (
            <button
              key={t.key}
              type="button"
              onClick={() => pick(t.key)}
              className={`w-full text-left px-2.5 py-1 text-[11px] transition-colors cursor-pointer ${
                cur?.role === t.key ? 'bg-accent/10 text-accent' : 'text-secondary hover:bg-elevated hover:text-foreground'
              }`}
            >
              {t.label}
              {cur?.role === t.key && <span className="float-right text-[9px] text-muted ml-3">当前</span>}
            </button>
          ))}
          {cur && (
            <>
              <div className="my-1 border-t border-border/60" />
              <button
                type="button"
                onClick={() => { toggleRoleMark(symbol, cur.role); setOpen(false) }}
                className="w-full text-left px-2.5 py-1 text-[11px] text-muted hover:bg-elevated hover:text-danger transition-colors cursor-pointer"
              >
                清除标注
              </button>
            </>
          )}
          <div className="px-2.5 py-0.5 text-[9px] text-muted border-t border-border/60 mt-1">
            {cur ? `标记于 ${cur.date}` : '仅本地保存'}
          </div>
        </div>
      )}
    </div>
  )
}