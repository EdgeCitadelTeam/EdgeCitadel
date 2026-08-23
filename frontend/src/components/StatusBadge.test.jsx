import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import StatusBadge from './StatusBadge'

describe('StatusBadge', () => {
  it('names an online status', () => {
    render(<StatusBadge status="online" />)
    expect(screen.getByRole('status', { name: 'Status: online' })).toBeInTheDocument()
  })

  it('defaults to offline', () => {
    render(<StatusBadge />)
    expect(screen.getByRole('status', { name: 'Status: offline' })).toBeInTheDocument()
  })
})
