import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createColumnHelper } from '@tanstack/react-table';
import { DataTable } from '../components/DataTable';

type Row = { name: string };
const ch = createColumnHelper<Row>();
const columns = [ch.accessor('name', { header: '名称', cell: (i) => i.getValue() })];
const data: Row[] = [{ name: 'alpha' }, { name: 'beta' }];

describe('DataTable row expansion', () => {
  it('calls onExpandRow(0) when first row is clicked and nothing is expanded', async () => {
    const user = userEvent.setup();
    const onExpandRow = vi.fn();
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowIndex={null}
        onExpandRow={onExpandRow}
        renderExpandedRow={() => <div>detail</div>}
      />,
    );
    const rows = screen.getAllByRole('row');
    // rows[0] is thead, rows[1] is first data row
    await user.click(rows[1]);
    expect(onExpandRow).toHaveBeenCalledWith(0);
  });

  it('calls onExpandRow(null) when the already-expanded row is clicked', async () => {
    const user = userEvent.setup();
    const onExpandRow = vi.fn();
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowIndex={0}
        onExpandRow={onExpandRow}
        renderExpandedRow={() => <div>detail</div>}
      />,
    );
    const rows = screen.getAllByRole('row');
    await user.click(rows[1]);
    expect(onExpandRow).toHaveBeenCalledWith(null);
  });

  it('renders the expanded panel only for the expanded row', () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowIndex={1}
        onExpandRow={vi.fn()}
        renderExpandedRow={(row) => <div>Detail: {row.name}</div>}
      />,
    );
    expect(screen.getByText('Detail: beta')).toBeInTheDocument();
    expect(screen.queryByText('Detail: alpha')).not.toBeInTheDocument();
  });

  it('does not show expanded content when expandedRowIndex is null', () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowIndex={null}
        onExpandRow={vi.fn()}
        renderExpandedRow={() => <div>should-not-show</div>}
      />,
    );
    expect(screen.queryByText('should-not-show')).not.toBeInTheDocument();
  });

  it('supports key-based expansion state for paginated tables', async () => {
    const user = userEvent.setup();
    const onExpandRowKeyChange = vi.fn();
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowKey={null}
        onExpandRowKeyChange={onExpandRowKeyChange}
        getExpandedRowKey={(row) => row.name}
        renderExpandedRow={() => <div>detail</div>}
      />, 
    );

    const rows = screen.getAllByRole('row');
    await user.click(rows[2]);
    expect(onExpandRowKeyChange).toHaveBeenCalledWith('beta');
  });

  it('supports keyboard activation for expandable rows', async () => {
    const user = userEvent.setup();
    const onExpandRow = vi.fn();
    render(
      <DataTable
        columns={columns}
        data={data}
        expandedRowIndex={null}
        onExpandRow={onExpandRow}
        renderExpandedRow={() => <div>detail</div>}
      />,
    );

    const rows = screen.getAllByRole('row');
    rows[1].focus();
    expect(rows[1]).toHaveAttribute('tabindex', '0');
    await user.keyboard('{Enter}');
    expect(onExpandRow).toHaveBeenCalledWith(0);
  });
});
