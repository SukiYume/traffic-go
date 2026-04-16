import { NavLink, useLocation } from 'react-router-dom';
import { useEffect, useMemo, type ReactNode } from 'react';
import clsx from 'clsx';

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/usage', label: 'Usage' },
  { to: '/processes', label: 'Processes' },
  { to: '/remotes', label: 'Remotes' },
  { to: '/forward', label: 'Forward' },
];

type RouteMeta = {
  title: string;
  subtitle: string;
};

const canonicalQueryAllowlist: Record<string, string[]> = {
  '/': ['range'],
  '/usage': ['range', 'comm', 'pid', 'exe', 'remoteIp', 'localPort', 'direction', 'proto', 'attribution'],
  '/processes': ['range'],
  '/remotes': ['range', 'direction', 'include_loopback'],
  '/forward': ['range', 'comm', 'pid', 'exe', 'remoteIp', 'localPort', 'direction', 'proto'],
};

const defaultRouteMeta: RouteMeta = {
  title: '流量总览',
  subtitle: '先看整体带宽与连接趋势，再下钻到进程、对端与端口定位异常。',
};

function resolveRouteMeta(pathname: string): RouteMeta {
  if (pathname === '/usage') {
    return {
      title: '流量明细',
      subtitle: '逐条检查连接五元组与归因状态，结合日志线索回溯来源和目标。',
    };
  }
  if (pathname === '/processes') {
    return {
      title: '进程聚合',
      subtitle: '按进程维度观察流量消耗与趋势，识别高占用和异常波动。',
    };
  }
  if (pathname === '/remotes') {
    return {
      title: '远端排行',
      subtitle: '聚合来源与目标 IP 热点，快速锁定高频通信对端。',
    };
  }
  if (pathname === '/forward') {
    return {
      title: '转发流量',
      subtitle: '核对 NAT 或转发表入口出口关系，排查异常转发路径。',
    };
  }
  return defaultRouteMeta;
}

function upsertMeta(type: 'name' | 'property', key: string, content: string) {
  let element = document.head.querySelector(`meta[${type}="${key}"]`) as HTMLMetaElement | null;
  if (!element) {
    element = document.createElement('meta');
    element.setAttribute(type, key);
    document.head.appendChild(element);
  }
  element.setAttribute('content', content);
}

function upsertCanonical(href: string) {
  let canonical = document.head.querySelector('link[rel="canonical"]') as HTMLLinkElement | null;
  if (!canonical) {
    canonical = document.createElement('link');
    canonical.setAttribute('rel', 'canonical');
    document.head.appendChild(canonical);
  }
  canonical.setAttribute('href', href);
}

function normalizePathname(pathname: string) {
  const trimmed = pathname.trim();
  if (!trimmed) return '/';
  return trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
}

function buildCanonicalUrl(pathnameRaw: string, searchRaw: string) {
  const pathname = normalizePathname(pathnameRaw);
  const allowlist = canonicalQueryAllowlist[pathname] ?? ['range'];
  const sourceParams = new URLSearchParams(searchRaw);
  const canonicalParams = new URLSearchParams();

  for (const key of allowlist) {
    const value = sourceParams.get(key);
    if (value && value.trim()) {
      canonicalParams.set(key, value.trim());
    }
  }

  const query = canonicalParams.toString();
  const pathWithQuery = query ? `${pathname}?${query}` : pathname;
  return `${window.location.origin}${pathWithQuery}`;
}

export function Layout({ children }: { children: ReactNode }) {
  const location = useLocation();
  const routeMeta = useMemo(() => resolveRouteMeta(location.pathname), [location.pathname]);

  useEffect(() => {
    const title = `traffic-go | ${routeMeta.title}`;
    const description = `traffic-go Web 控制台：${routeMeta.subtitle}`;
    const canonical = buildCanonicalUrl(location.pathname, location.search);

    document.title = title;
    upsertMeta('name', 'description', description);
    upsertMeta('name', 'application-name', 'traffic-go');
    upsertMeta('name', 'apple-mobile-web-app-title', 'traffic-go');
    upsertMeta('property', 'og:title', title);
    upsertMeta('property', 'og:description', description);
    upsertMeta('property', 'og:type', 'website');
    upsertMeta('property', 'og:locale', 'zh_CN');
    upsertMeta('property', 'og:url', canonical);
    upsertMeta('name', 'twitter:title', title);
    upsertMeta('name', 'twitter:description', description);
    upsertCanonical(canonical);
  }, [routeMeta, location.pathname, location.search]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">
            <span className="brand-mark-core">TG</span>
          </div>
          <div>
            <h1>traffic-go</h1>
            <p>进程、端口、对端 IP 与方向分离回查</p>
          </div>
        </div>
        <nav className="nav">
          {links.map((link) => (
            <NavLink
              key={link.to}
              to={link.to}
              className={({ isActive }) => clsx('nav-link', isActive && 'active')}
            >
              {link.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-note">
          <strong>范围规则</strong>
          <span>超过 30 天时会切到小时聚合，PID / EXE 会自动失效。</span>
        </div>
      </aside>
      <main className="content">{children}</main>
    </div>
  );
}
