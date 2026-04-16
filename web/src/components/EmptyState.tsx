export function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="empty-state">
      <div className="empty-state-icon">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2ZM13 17H11V15H13V17ZM13 13H11V7H13V13Z" fill="url(#paint0_linear)"/>
          <defs>
            <linearGradient id="paint0_linear" x1="12" y1="2" x2="12" y2="22" gradientUnits="userSpaceOnUse">
              <stop stopColor="#38BDF8" stopOpacity="0.8"/>
              <stop offset="1" stopColor="#818CF8" stopOpacity="0.4"/>
            </linearGradient>
          </defs>
        </svg>
      </div>
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}
