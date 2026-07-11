import { useState } from 'react';

export function useExactTotal(scopeKey: string) {
  const [requestedScopeKey, setRequestedScopeKey] = useState<string | null>(null);
  return {
    includeTotal: requestedScopeKey === scopeKey,
    requestExactTotal: () => setRequestedScopeKey(scopeKey),
  };
}
