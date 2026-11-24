import { lazy } from 'react'
import { Navigate, Route, Routes } from 'react-router'

const StockResearchPanel = lazy(() => import('./panel'))

export function LocalResearchRoutes() {
  return <Routes>
    <Route path="/chat" element={<main className="h-dvh overflow-y-auto bg-background text-foreground">
      <header className="flex items-center justify-between border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold">个人投研工作台</h1>
        <span className="rounded-full bg-muted px-3 py-1 text-xs text-muted-foreground">本机单用户</span>
      </header>
      <div className="p-6"><StockResearchPanel /></div>
    </main>} />
    <Route path="*" element={<Navigate to="/chat" replace />} />
  </Routes>
}
