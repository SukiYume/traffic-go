import { useEffect, useId, useRef, useState } from 'react';

type Option = { value: string; label: string };

export function CustomSelect({
  value,
  options,
  onChange,
  disabled = false,
  title,
}: {
  value: string;
  options: Option[];
  onChange: (value: string) => void;
  disabled?: boolean;
  title?: string;
}) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const ref = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const listboxRef = useRef<HTMLUListElement>(null);
  const listboxId = useId();
  const selectedIndex = Math.max(
    0,
    options.findIndex((option) => option.value === value),
  );

  const closeDropdown = (focusTrigger = false) => {
    setOpen(false);
    if (focusTrigger) {
      buttonRef.current?.focus();
    }
  };

  const openDropdown = (nextIndex = selectedIndex) => {
    if (disabled) return;
    setActiveIndex(Math.min(Math.max(nextIndex, 0), options.length - 1));
    setOpen(true);
  };

  const commitOption = (index: number) => {
    const option = options[index];
    if (!option) return;
    onChange(option.value);
    closeDropdown(true);
  };

  useEffect(() => {
    if (!open) return;
    const onMouseDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        closeDropdown();
      }
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeDropdown(true);
    };
    const focusFrame = window.requestAnimationFrame(() => {
      listboxRef.current?.focus();
    });
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open]);

  const selected = options.find((o) => o.value === value);
  const optionId = (index: number) => `${listboxId}-option-${index}`;

  return (
    <div className="custom-select" ref={ref}>
      <button
        type="button"
        ref={buttonRef}
        className="custom-select-trigger"
        onClick={() => {
          if (open) {
            closeDropdown();
            return;
          }
          openDropdown();
        }}
        onKeyDown={(event) => {
          if (disabled) return;
          switch (event.key) {
            case 'ArrowDown':
              event.preventDefault();
              openDropdown(selectedIndex);
              break;
            case 'ArrowUp':
              event.preventDefault();
              openDropdown(options.length - 1);
              break;
            case 'Enter':
            case ' ':
              event.preventDefault();
              openDropdown(selectedIndex);
              break;
            case 'Home':
              event.preventDefault();
              openDropdown(0);
              break;
            case 'End':
              event.preventDefault();
              openDropdown(options.length - 1);
              break;
            default:
              break;
          }
        }}
        aria-haspopup="listbox"
        aria-controls={listboxId}
        aria-expanded={open}
        disabled={disabled}
        title={title}
      >
        <span>{selected?.label ?? ''}</span>
        <span className="custom-select-arrow" aria-hidden="true">
          {open ? '▲' : '▼'}
        </span>
      </button>
      {open && (
        <ul
          id={listboxId}
          ref={listboxRef}
          className="custom-select-dropdown"
          role="listbox"
          tabIndex={-1}
          aria-activedescendant={optionId(activeIndex)}
          onKeyDown={(event) => {
            switch (event.key) {
              case 'ArrowDown':
                event.preventDefault();
                setActiveIndex((current) => Math.min(current + 1, options.length - 1));
                break;
              case 'ArrowUp':
                event.preventDefault();
                setActiveIndex((current) => Math.max(current - 1, 0));
                break;
              case 'Home':
                event.preventDefault();
                setActiveIndex(0);
                break;
              case 'End':
                event.preventDefault();
                setActiveIndex(options.length - 1);
                break;
              case 'Enter':
              case ' ':
                event.preventDefault();
                commitOption(activeIndex);
                break;
              case 'Tab':
                closeDropdown();
                break;
              default:
                break;
            }
          }}
        >
          {options.map((option, index) => (
            <li
              key={option.value}
              id={optionId(index)}
              role="option"
              aria-selected={option.value === value}
              className={`custom-select-option${option.value === value ? ' selected' : ''}${index === activeIndex ? ' active' : ''}`}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => commitOption(index)}
            >
              {option.label}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
