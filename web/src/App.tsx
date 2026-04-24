import { Navigate, Route, Routes } from 'react-router-dom';
import { Layout } from './components/Layout';
import { DashboardPage } from './pages/DashboardPage';
import { ForwardPage } from './pages/ForwardPage';
import { HistoryPage } from './pages/HistoryPage';
import { ProcessesPage } from './pages/ProcessesPage';
import { RemotesPage } from './pages/RemotesPage';
import { UsagePage } from './pages/UsagePage';

export function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/usage" element={<UsagePage />} />
        <Route path="/processes" element={<ProcessesPage />} />
        <Route path="/remotes" element={<RemotesPage />} />
        <Route path="/forward" element={<ForwardPage />} />
        <Route path="/history" element={<HistoryPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  );
}
