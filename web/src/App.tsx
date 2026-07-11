import { Component, lazy, Suspense, type ErrorInfo, type ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { Layout } from './components/Layout';

const DashboardPage = lazy(async () => ({ default: (await import('./pages/DashboardPage')).DashboardPage }));
const ForwardPage = lazy(async () => ({ default: (await import('./pages/ForwardPage')).ForwardPage }));
const HistoryPage = lazy(async () => ({ default: (await import('./pages/HistoryPage')).HistoryPage }));
const ProcessesPage = lazy(async () => ({ default: (await import('./pages/ProcessesPage')).ProcessesPage }));
const RemotesPage = lazy(async () => ({ default: (await import('./pages/RemotesPage')).RemotesPage }));
const UsagePage = lazy(async () => ({ default: (await import('./pages/UsagePage')).UsagePage }));

export class RouteErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error: unknown, info: ErrorInfo) {
    console.error('route rendering failed', error, info.componentStack);
  }

  render() {
    if (this.state.failed) {
      return (
        <div className="empty-state" role="alert">
          <strong>页面资源加载失败</strong>
          <span>应用可能刚刚完成升级。请刷新页面以加载最新资源。</span>
          <button type="button" className="chip" onClick={() => window.location.reload()}>刷新页面</button>
        </div>
      );
    }
    return this.props.children;
  }
}

export function App() {
  return (
    <Layout>
      <RouteErrorBoundary>
        <Suspense fallback={<div className="route-loading" role="status">页面加载中…</div>}>
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/usage" element={<UsagePage />} />
            <Route path="/processes" element={<ProcessesPage />} />
            <Route path="/remotes" element={<RemotesPage />} />
            <Route path="/forward" element={<ForwardPage />} />
            <Route path="/history" element={<HistoryPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Suspense>
      </RouteErrorBoundary>
    </Layout>
  );
}
