import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CustomSelect } from '../components/CustomSelect';

const OPTIONS = [
  { value: '', label: '全部方向' },
  { value: 'in', label: '入站' },
  { value: 'out', label: '出站' },
];

describe('CustomSelect', () => {
  it('shows the label of the selected option', () => {
    render(<CustomSelect value="in" options={OPTIONS} onChange={() => {}} />);
    expect(screen.getByRole('button')).toHaveTextContent('入站');
  });

  it('shows the first option label when value is empty string', () => {
    render(<CustomSelect value="" options={OPTIONS} onChange={() => {}} />);
    expect(screen.getByRole('button')).toHaveTextContent('全部方向');
  });

  it('opens the dropdown when trigger is clicked', async () => {
    const user = userEvent.setup();
    render(<CustomSelect value="" options={OPTIONS} onChange={() => {}} />);
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button'));
    expect(screen.getByRole('listbox')).toBeInTheDocument();
    expect(screen.getAllByRole('option')).toHaveLength(3);
  });

  it('calls onChange and closes when an option is clicked', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<CustomSelect value="" options={OPTIONS} onChange={onChange} />);
    await user.click(screen.getByRole('button'));
    await user.click(screen.getByRole('option', { name: '入站' }));
    expect(onChange).toHaveBeenCalledWith('in');
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
  });

  it('marks the selected option with aria-selected', async () => {
    const user = userEvent.setup();
    render(<CustomSelect value="out" options={OPTIONS} onChange={() => {}} />);
    await user.click(screen.getByRole('button'));
    const outOption = screen.getByRole('option', { name: '出站' });
    expect(outOption).toHaveAttribute('aria-selected', 'true');
    const inOption = screen.getByRole('option', { name: '入站' });
    expect(inOption).toHaveAttribute('aria-selected', 'false');
  });

  it('closes when Escape is pressed', async () => {
    const user = userEvent.setup();
    render(<CustomSelect value="" options={OPTIONS} onChange={() => {}} />);
    await user.click(screen.getByRole('button'));
    expect(screen.getByRole('listbox')).toBeInTheDocument();
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
  });
});
