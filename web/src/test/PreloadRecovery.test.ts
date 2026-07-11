import { describe, expect, it, vi } from 'vitest';
import { installPreloadErrorRecovery } from '../preload-recovery';

describe('preload error recovery', () => {
  it('reloads once and preserves a cooldown across document runtimes', () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => { values.set(key, value); },
    };
    let current = 100_000;
    const reload = vi.fn();
    const firstTarget = new EventTarget();
    const removeFirst = installPreloadErrorRecovery({
      target: firstTarget,
      storage,
      reload,
      now: () => current,
    });

    const first = new Event('vite:preloadError', { cancelable: true });
    firstTarget.dispatchEvent(first);
    firstTarget.dispatchEvent(new Event('vite:preloadError', { cancelable: true }));
    expect(first.defaultPrevented).toBe(true);
    expect(reload).toHaveBeenCalledTimes(1);
    removeFirst();

    current += 1_000;
    const secondTarget = new EventTarget();
    const removeSecond = installPreloadErrorRecovery({ target: secondTarget, storage, reload, now: () => current });
    secondTarget.dispatchEvent(new Event('vite:preloadError', { cancelable: true }));
    expect(reload).toHaveBeenCalledTimes(1);
    removeSecond();

    current += 60_001;
    const thirdTarget = new EventTarget();
    installPreloadErrorRecovery({ target: thirdTarget, storage, reload, now: () => current });
    thirdTarget.dispatchEvent(new Event('vite:preloadError', { cancelable: true }));
    expect(reload).toHaveBeenCalledTimes(2);
  });

  it('does not reload when a cross-document cooldown cannot be persisted', () => {
    const reload = vi.fn();
    const target = new EventTarget();
    installPreloadErrorRecovery({ target, storage: null, reload, now: () => 100_000 });
    const noStorageEvent = new Event('vite:preloadError', { cancelable: true });
    target.dispatchEvent(noStorageEvent);
    expect(noStorageEvent.defaultPrevented).toBe(false);
    expect(reload).not.toHaveBeenCalled();

    const throwingStorage = {
      getItem: () => null,
      setItem: () => { throw new Error('storage disabled'); },
    };
    const secondTarget = new EventTarget();
    installPreloadErrorRecovery({ target: secondTarget, storage: throwingStorage, reload, now: () => 100_000 });
    const writeFailureEvent = new Event('vite:preloadError', { cancelable: true });
    secondTarget.dispatchEvent(writeFailureEvent);
    expect(writeFailureEvent.defaultPrevented).toBe(false);
    expect(reload).not.toHaveBeenCalled();
  });
});
