function normalizeBasePath(pathname: string) {
  if (!pathname || pathname === '/') {
    return '';
  }
  return pathname.endsWith('/') ? pathname.slice(0, -1) : pathname;
}

export function deriveAppBasePath(moduleUrl: string) {
  try {
    const assetPath = new URL(moduleUrl, window.location.origin).pathname;
    const assetIndex = assetPath.lastIndexOf('/assets/');
    if (assetIndex >= 0) {
      return normalizeBasePath(assetPath.slice(0, assetIndex));
    }
  } catch {
    // Fall back to root below when the module URL cannot be parsed.
  }
  return '';
}

const APP_BASE_PATH = deriveAppBasePath(import.meta.url);

export function appBasePath() {
  return APP_BASE_PATH;
}

export function routerBasename() {
  return APP_BASE_PATH || undefined;
}

export function withAppBase(path: string) {
  return `${APP_BASE_PATH}${path}`;
}
