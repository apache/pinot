module.exports = {
  globals: {
    server: true
  },
  root: true,
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module'
  },
  plugins: ['prettier', 'ember'],
  extends: ['eslint:recommended', 'plugin:ember/recommended', "plugin:prettier/recommended"],
  env: {
    browser: true,
    es6: true,
    node: true
  },
  rules: {
    'no-console': 'error',
    'no-debugger': 'error',
    'eol-last': ['error', 'always'],
    'indent': ['error', 2, { 'SwitchCase': 1 }],
    'space-in-parens': ['error', 'never'],
    'no-trailing-spaces': ['error'],
    'comma-dangle': ['error', 'never'],
    'comma-spacing': ['error', { before: false, after: true }],
    semi: ['error', 'always'],
    'comma-style': ['error', 'last'],
    'no-unused-vars': ['error', { vars: 'all', args: 'after-used', ignoreRestSiblings: false }],
    "space-before-function-paren": ["error", {
      "anonymous": "always",
      "named": "never",
      "asyncArrow": "always"
    }],
    // TODO: fix all errorings and change rules back to "error"
    'ember/avoid-leaking-state-in-ember-objects': ['error'],
    'ember/no-attrs-in-components': ['error'],
    'prettier/prettier': ['error']
  },
  overrides: [
    // node files
    {
      files: [
        'ember-cli-build.js',
        'testem.js',
        'config/**/*.js',
        'lib/*/index.js'
      ],
      parserOptions: {
        sourceType: 'script',
        ecmaVersion: 2015
      },
      env: {
        browser: false,
        node: true
      }
    },

    // test files
    {
      files: ['tests/**/*.js'],
      excludedFiles: ['tests/dummy/**/*.js'],
      env: {
        embertest: true,
        browser: false,
        node: true
      }
    }
  ]
};
