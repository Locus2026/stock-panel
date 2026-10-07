import { useState, useCallback, useEffect, useRef } from 'react'
import { storage } from '@/lib/storage'

// 旧版按周期隔离的双池 ('strategy-pool' 日线 + 'strategy-pool-1m' 分钟) 已合并为
// 统一池 ('strategy-pool'): 策略自带周期声明, 执行时按各自 timeframes 路由,
// 池不再按周期隔离。此处做一次性迁移: 日线在前、分钟在后、按 ID 去重,
// 完成后移除旧分钟 key 保证幂等 (StrictMode 双调用 / HMR 重放均安全)。
function loadUnifiedPool(): string[] {
  const minute = storage.strategyPoolMinute.get([])
  const daily = storage.strategyPool.get([])
  if (minute.length === 0) {
    storage.strategyPoolMinute.remove()
    return daily
  }
  const merged = [...daily]
  for (const id of minute) {
    if (!merged.includes(id)) merged.push(id)
  }
  storage.strategyPool.set(merged)
  storage.strategyPoolMinute.remove()
  return merged
}

/**
 * 策略池 — 日线与分钟策略共用的统一池。
 * 卡片列表可按周期筛选显示, 但池本身只有一份;
 * addToPool 不再区分周期 (构建器/叠加策略创建的日线策略与分钟策略同池)。
 */
export function useStrategyPool() {
  const [pool, setPool] = useState<string[]>(loadUnifiedPool)
  // 本会话内新建/加入池的策略 ID: **不参与 prune**。
  // 背景 (2026-10-06): 叠加策略保存后, 若主查询缓存尚未包含它, prune 会把它
  // 当"后端已不存在的失效策略"永久移出池并写回 localStorage —— 用户看到的是
  // "策略静默消失", 刷新也回不来。引擎 reload 瞬时跳过某策略时同样会误删。
  // 保护期仅限当前页面会话: 刷新后 clearProtected() 重置, 失效策略照常清理。
  const protectedIds = useRef<Set<string>>(new Set())

  useEffect(() => {
    storage.strategyPool.set(pool)
  }, [pool])

  const addToPool = useCallback((id: string) => {
    protectedIds.current.add(id)
    setPool(prev => (prev.includes(id) ? prev : [...prev, id]))
  }, [])

  const removeFromPool = useCallback((id: string) => {
    protectedIds.current.delete(id)
    setPool(prev => prev.filter(x => x !== id))
  }, [])

  const reorderPool = useCallback((newOrder: string[]) => {
    setPool(newOrder)
  }, [])

  /** 清空新建保护期 (页面挂载时调用): 跨会话后失效策略可被正常清理 */
  const clearProtected = useCallback(() => {
    protectedIds.current.clear()
  }, [])

  // 清除池中不存在于 validIds 的失效策略(如本地开发残留的自定义策略)。
  // 调用方传入"全周期合并的策略列表" ID, 池内日线/分钟策略一并校验。
  // 仅当确实有失效项时才更新,避免无谓重渲染。
  // 本会话内新加入的 ID 跳过(见 protectedIds 注释)。
  const prune = useCallback((validIds: Iterable<string>) => {
    const validSet = validIds instanceof Set ? validIds : new Set(validIds)
    const keep = protectedIds.current
    setPool(prev => {
      if (prev.length === 0) return prev
      const next = prev.filter(id => validSet.has(id) || keep.has(id))
      return next.length === prev.length ? prev : next
    })
  }, [])

  const isInPool = useCallback((id: string) => pool.includes(id), [pool])

  return { pool, addToPool, removeFromPool, reorderPool, prune, isInPool, clearProtected }
}
