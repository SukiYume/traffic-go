import { useEffect, useRef, useState } from 'react';

export function useResettingPage(resetKey: string, initialPage = 1) {
  const [page, setPage] = useState(initialPage);
  const previousResetKey = useRef(resetKey);
  const shouldReset = previousResetKey.current !== resetKey;
  const effectivePage = shouldReset ? initialPage : page;

  useEffect(() => {
    if (!shouldReset) {
      return;
    }
    previousResetKey.current = resetKey;
    setPage(initialPage);
  }, [initialPage, resetKey, shouldReset]);

  return [effectivePage, setPage] as const;
}
