/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: '#0f1117',
          50: '#1a1d2e',
          100: '#232738',
          200: '#2d3142',
          300: '#3a3f54',
        },
        accent: {
          DEFAULT: '#6366f1',
          light: '#818cf8',
          dark: '#4f46e5',
        },
        status: {
          online: '#22c55e',
          offline: '#6b7280',
          error: '#ef4444',
          warning: '#f59e0b',
        },
        skill: {
          chat:      { bg: 'rgba(120,140,180,0.12)', text: '#9aa9c2' },
          summarize: { bg: 'rgba(96,165,250,0.16)',  text: '#60a5fa' },
          classify:  { bg: 'rgba(251,191,36,0.16)',  text: '#fbbf24' },
          explain:   { bg: 'rgba(74,222,128,0.16)',  text: '#4ade80' },
        },
      },
    },
  },
  plugins: [],
}
