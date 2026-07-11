import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from './App';
import { ApiProvider } from './api-context';
import { createAppApiClient } from './api';
import { routerBasename } from './base-path';
import { installPreloadErrorRecovery } from './preload-recovery';
import './styles.css';

installPreloadErrorRecovery();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: true,
      retry: 1,
    },
  },
});

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ApiProvider client={createAppApiClient()}>
        <BrowserRouter basename={routerBasename()} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <App />
        </BrowserRouter>
      </ApiProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
