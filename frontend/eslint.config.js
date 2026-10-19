import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'

const runtimeGlobals = Object.assign({}, globals.browser, globals.node)
const hookRules = Object.assign({}, reactHooks.configs.recommended.rules, {
  'no-undef': 'error',
  'no-dupe-keys': 'error',
  'no-unreachable': 'error',
})

export default [
  {
    ignores: ['dist', 'coverage', 'node_modules'],
  },
  {
    files: ['src/**/*.{js,jsx}', 'vite.config.js'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
      globals: runtimeGlobals,
    },
    plugins: {
      'react-hooks': reactHooks,
    },
    rules: hookRules,
  },
]
