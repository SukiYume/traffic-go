import { Fragment } from 'react';
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type OnChangeFn,
  type SortingState,
} from '@tanstack/react-table';
import type { ReactNode } from 'react';
import { formatNumber } from '../utils';

type PaginationState = {
  page: number;
  pageSize: number;
  totalRows: number;
  onPageChange: (page: number) => void;
};

type TableColumnMeta = {
  className?: string;
  headerClassName?: string;
  cellClassName?: string;
  align?: 'left' | 'center' | 'right';
  nowrap?: boolean;
};

function columnMetaToClassName(meta: TableColumnMeta | undefined, target: 'header' | 'cell') {
  if (!meta) return undefined;
  const classes = [meta.className];
  if (target === 'header' && meta.headerClassName) classes.push(meta.headerClassName);
  if (target === 'cell' && meta.cellClassName) classes.push(meta.cellClassName);
  if (meta.align) classes.push(`align-${meta.align}`);
  if (meta.nowrap) classes.push('cell-nowrap');
  const filtered = classes.filter(Boolean);
  return filtered.length ? filtered.join(' ') : undefined;
}

export function DataTable<TData>({
  columns,
  data,
  tableClassName,
  emptyText = '暂无数据',
  sorting,
  onSortingChange,
  manualSorting = false,
  pagination,
  onRowClick,
  isRowSelected,
  expandedRowIndex,
  onExpandRow,
  renderExpandedRow,
}: {
  columns: ColumnDef<TData, any>[];
  data: TData[];
  tableClassName?: string;
  emptyText?: string;
  sorting?: SortingState;
  onSortingChange?: OnChangeFn<SortingState>;
  manualSorting?: boolean;
  pagination?: PaginationState;
  onRowClick?: (row: TData) => void;
  isRowSelected?: (row: TData) => boolean;
  expandedRowIndex?: number | null;
  onExpandRow?: (index: number | null) => void;
  renderExpandedRow?: (row: TData) => ReactNode;
}) {
  const table = useReactTable({
    columns,
    data,
    state: sorting ? { sorting } : {},
    onSortingChange,
    manualSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: manualSorting ? undefined : getSortedRowModel(),
  });

  const totalPages = pagination ? Math.max(1, Math.ceil(pagination.totalRows / pagination.pageSize)) : 1;

  return (
    <div className="table-card">
      <div className="table-wrap">
        <table className={tableClassName ? `table ${tableClassName}` : 'table'}>
          <thead>
            {table.getHeaderGroups().map((group) => (
              <tr key={group.id}>
                {group.headers.map((header) => {
                  const meta = (header.column.columnDef.meta as TableColumnMeta | undefined) ?? undefined;
                  const canSort = header.column.getCanSort();
                  const sortState = header.column.getIsSorted();
                  return (
                    <th key={header.id} className={columnMetaToClassName(meta, 'header')}>
                      {header.isPlaceholder ? null : canSort ? (
                        <button
                          type="button"
                          className="table-sort"
                          onClick={header.column.getToggleSortingHandler()}
                        >
                          <span>{flexRender(header.column.columnDef.header, header.getContext())}</span>
                          <span className="table-sort-indicator">
                            {sortState === 'asc' ? '↑' : sortState === 'desc' ? '↓' : '↕'}
                          </span>
                        </button>
                      ) : (
                        flexRender(header.column.columnDef.header, header.getContext())
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => {
                const rowIndex = Number(row.id);
                const isExpanded = expandedRowIndex === rowIndex && renderExpandedRow != null;
                const handleClick = renderExpandedRow && onExpandRow
                  ? () => {
                      if (window.getSelection()?.toString()) return;
                      onExpandRow(isExpanded ? null : rowIndex);
                    }
                  : onRowClick
                    ? () => {
                        if (window.getSelection()?.toString()) return;
                        onRowClick(row.original);
                      }
                    : undefined;

                return (
                  <Fragment key={row.id}>
                    <tr
                      className={isRowSelected?.(row.original) ? 'selected' : isExpanded ? 'selected' : undefined}
                      onClick={handleClick}
                      style={handleClick ? { cursor: 'pointer' } : undefined}
                    >
                      {row.getVisibleCells().map((cell) => {
                        const meta = (cell.column.columnDef.meta as TableColumnMeta | undefined) ?? undefined;
                        return (
                          <td key={cell.id} className={columnMetaToClassName(meta, 'cell')}>
                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                          </td>
                        );
                      })}
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={columns.length} onClick={(e) => e.stopPropagation()}>
                          {renderExpandedRow(row.original)}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })
            ) : (
              <tr>
                <td colSpan={columns.length}>
                  <div className="empty-inline">{emptyText}</div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {pagination ? (
        <footer className="table-footer">
          <span>
            第 {pagination.page} / {formatNumber(totalPages)} 页，共 {formatNumber(pagination.totalRows)} 条
          </span>
          <div className="table-footer-actions">
            <button type="button" className="chip ghost" onClick={() => pagination.onPageChange(1)} disabled={pagination.page <= 1}>
              首页
            </button>
            <button
              type="button"
              className="chip ghost"
              onClick={() => pagination.onPageChange(pagination.page - 1)}
              disabled={pagination.page <= 1}
            >
              上一页
            </button>
            <button
              type="button"
              className="chip ghost"
              onClick={() => pagination.onPageChange(pagination.page + 1)}
              disabled={pagination.page >= totalPages}
            >
              下一页
            </button>
          </div>
        </footer>
      ) : null}
    </div>
  );
}
