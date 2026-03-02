import clsx from 'clsx'

const statusColors = {
  online: 'bg-status-online',
  offline: 'bg-status-offline',
  error: 'bg-status-error',
  warning: 'bg-status-warning',
}

const glowColors = {
  online: 'shadow-[0_0_8px_rgba(34,197,94,0.6)]',
  error: 'shadow-[0_0_8px_rgba(239,68,68,0.6)]',
  warning: 'shadow-[0_0_8px_rgba(245,158,11,0.6)]',
}

export default function StatusBadge({ status, size = 'sm', label }) {
  const sizeClass = size === 'sm' ? 'w-2.5 h-2.5' : 'w-3.5 h-3.5'

  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className={clsx(
          'rounded-full inline-block',
          sizeClass,
          statusColors[status] || statusColors.offline,
          glowColors[status]
        )}
      />
      {label && <span className="text-xs text-gray-400">{label}</span>}
    </span>
  )
}
