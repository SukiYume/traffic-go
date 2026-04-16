import { NavLink } from 'react-router-dom';
import type { ReactNode } from 'react';
import clsx from 'clsx';

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/usage', label: 'Usage' },
  { to: '/processes', label: 'Processes' },
  { to: '/remotes', label: 'Remotes' },
  { to: '/forward', label: 'Forward' },
];

export function Layout({ children }: { children: ReactNode }) {
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
