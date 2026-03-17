---
paths:
  - "frontend/src/**/*.jsx"
  - "frontend/src/**/*.js"
  - "frontend/src/**/*.css"
---

# React Frontend Rules

## Components
- Functional components with hooks only (no class components)
- One component per file, file name matches component name
- Props destructured in function signature
- Keep components under 150 lines — extract sub-components if longer

## State Management
- Zustand for global state (`stores/appStore.js`)
- Local state with `useState` for UI-only state (modals, form inputs)
- Never use Redux or React Context for state management
- Store mutations must be immutable (spread operator, not mutation)

## Styling
- Tailwind CSS utility classes exclusively
- No custom CSS files or CSS modules
- Custom colors defined in `tailwind.config.js` (surface, accent, status)
- Dark theme is default — always design dark-first

## Icons & UI
- `lucide-react` for all icons
- `react-hot-toast` for notifications
- `recharts` for data visualization
- `react-force-graph-2d` for network graph

## Data Fetching
- `axios` via `api/client.js` for REST calls
- Native WebSocket via `hooks/useWebSocket.js` for real-time
- No authentication headers needed for frontend API calls

## Conventions
- ES modules only (`import/export`, never `require`)
- No `console.log` in production code (use toast for user feedback)
- Clean up event listeners and subscriptions in `useEffect` return
- Use optional chaining (`?.`) for nullable data from API
