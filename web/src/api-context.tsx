import { createContext, useContext, type ReactNode } from 'react';
import type { TrafficApiClient } from './types';

const ApiContext = createContext<TrafficApiClient | null>(null);

export function ApiProvider({ client, children }: { client: TrafficApiClient; children: ReactNode }) {
  return <ApiContext.Provider value={client}>{children}</ApiContext.Provider>;
}

export function useApiClient() {
  const client = useContext(ApiContext);
  if (!client) {
    throw new Error('ApiProvider is missing');
  }
  return client;
}
