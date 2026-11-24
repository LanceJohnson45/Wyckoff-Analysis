import { StrictMode, Suspense } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import './app.css'
import { ErrorBoundary } from '@/components/error-boundary'
import { WyckoffLoading } from '@/components/loading'
import { PreferencesProvider } from '@/lib/preferences'
import { LocalResearchRoutes } from '@/features/stock-research/local'

const client = new QueryClient({ defaultOptions: { queries: { staleTime: 30_000, retry: 1 } } })

createRoot(document.getElementById('root')!).render(
  <StrictMode><ErrorBoundary><QueryClientProvider client={client}><PreferencesProvider>
    <BrowserRouter><Suspense fallback={<WyckoffLoading />}><LocalResearchRoutes /></Suspense></BrowserRouter>
  </PreferencesProvider></QueryClientProvider></ErrorBoundary></StrictMode>,
)
