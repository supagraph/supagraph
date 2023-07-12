module.exports = {
  env: {
    node: true,
  },
  extends: [
    'turbo',
    'airbnb',
    'airbnb-typescript',
    'plugin:import/recommended',
    'plugin:import/typescript',
    'plugin:prettier/recommended'
  ],
  plugins: ['@typescript-eslint', 'import', 'prettier'],
  settings: {
    "react": {
      "version": "18.2.0"
    },
    'import/parsers': {
      '@typescript-eslint/parser': ['.ts'],
    },
    'import/resolver': {
      typescript: {
        alwaysTryTypes: true,
        project: ['*/tsconfig.json'],
      },
    },
  },
  rules: {
    'turbo/no-undeclared-env-vars': 'off',
    'import/no-extraneous-dependencies': 'off',
    'import/prefer-default-export': 'off',
  },
  ignorePatterns: [
    '**/*.js',
    '**/*.json',
    'node_modules',
    'public',
    'styles',
    'coverage',
    'dist',
    '.turbo',
  ],
  root: true,
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: './tsconfig.json',
  },
}
