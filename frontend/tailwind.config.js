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
      },
    },
  },
  plugins: [],
}
