import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from './App';
import { ApiProvider } from './api-context';
import { createAppApiClient } from './api';
import { routerBasename } from './base-path';
import './styles.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ApiProvider client={createAppApiClient()}>
        <BrowserRouter basename={routerBasename()}>
          <App />
        </BrowserRouter>
      </ApiProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
