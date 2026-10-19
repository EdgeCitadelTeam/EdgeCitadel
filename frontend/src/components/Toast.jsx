import { Toaster } from 'react-hot-toast'

export default function Toast() {
  return (
    <Toaster
      position="top-right"
      toastOptions={{
        duration: 4000,
        style: {
          background: '#1a1d2e',
          color: '#e5e7eb',
          border: '1px solid #2d3142',
          fontSize: '14px',
        },
        success: {
          iconTheme: { primary: '#22c55e', secondary: '#1a1d2e' },
        },
        error: {
          iconTheme: { primary: '#ef4444', secondary: '#1a1d2e' },
          duration: 6000,
        },
      }}
    />
  )
}
