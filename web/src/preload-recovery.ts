const PRELOAD_RELOAD_KEY = 'traffic-go:preload-reload-at';
const PRELOAD_RELOAD_COOLDOWN_MS = 60_000;

type ListenerTarget = {
  addEventListener(type: string, listener: EventListenerOrEventListenerObject): void;
  removeEventListener(type: string, listener: EventListenerOrEventListenerObject): void;
};

type RecoveryOptions = {
  target?: ListenerTarget;
  storage?: Pick<Storage, 'getItem' | 'setItem'> | null;
  reload?: () => void;
  now?: () => number;
};

function defaultSessionStorage(): Pick<Storage, 'getItem' | 'setItem'> | undefined {
  try {
    return window.sessionStorage;
  } catch {
    return undefined;
  }
}

// Vite raises `vite:preloadError` when a tab that survived a deployment tries
// to load a now-deleted lazy chunk. Reload at most once per cooldown window so
// the fresh index can select current hashes without creating a reload loop for
// genuine network or packaging failures.
export function installPreloadErrorRecovery(options: RecoveryOptions = {}) {
  const target = options.target ?? window;
  const storage = options.storage === undefined ? defaultSessionStorage() : options.storage ?? undefined;
  const reload = options.reload ?? (() => window.location.reload());
  const now = options.now ?? Date.now;
  let reloadStarted = false;

  const onPreloadError: EventListener = (event) => {
    if (reloadStarted || !storage) {
      return;
    }

    const current = now();
    let lastReload = 0;
    try {
      lastReload = Number(storage.getItem(PRELOAD_RELOAD_KEY) ?? 0);
    } catch {
      // Without a cross-document guard, reloading can loop forever. Let the
      // original lazy-load failure reach the route error boundary instead.
      return;
    }
    if (Number.isFinite(lastReload) && lastReload > 0 && current - lastReload < PRELOAD_RELOAD_COOLDOWN_MS) {
      return;
    }

    try {
      storage.setItem(PRELOAD_RELOAD_KEY, String(current));
    } catch {
      return;
    }
    event.preventDefault();
    reloadStarted = true;
    reload();
  };

  target.addEventListener('vite:preloadError', onPreloadError);
  return () => target.removeEventListener('vite:preloadError', onPreloadError);
}
