import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import { FiltersBar } from '../components/FiltersBar';

describe('FiltersBar', () => {
  it('dedupes repeated process names in the datalist suggestions', () => {
    const { container } = render(
      <FiltersBar
        processes={[
          { pid: 1, comm: 'nginx', exe: '/usr/sbin/nginx', totalBytes: 0 },
          { pid: 2, comm: 'nginx', exe: '/usr/sbin/nginx', totalBytes: 0 },
          { pid: 3, comm: 'sshd', exe: '/usr/sbin/sshd', totalBytes: 0 },
        ]}
        filters={{
          comm: '',
          pid: '',
          exe: '',
          remoteIp: '',
          localPort: '',
          direction: '',
          proto: '',
          attribution: '',
        }}
        onChange={vi.fn()}
      />,
    );

    const options = [...container.querySelectorAll('#traffic-processes option')].map((option) =>
      option.getAttribute('value'),
    );
    expect(options).toEqual(['nginx', 'sshd']);
  });
});
