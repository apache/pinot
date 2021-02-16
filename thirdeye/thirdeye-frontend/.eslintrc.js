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
    'eol-last': ['warn', 'always'],
    'indent': ['warn', 2, { 'SwitchCase': 1 }],
    'space-in-parens': ['warn', 'never'],
    'no-trailing-spaces': ['warn'],
    'comma-dangle': ['warn', 'never'],
    'comma-spacing': ['warn', { before: false, after: true }],
    semi: ['warn', 'always'],
    'comma-style': ['warn', 'last'],
    'no-unused-vars': ['warn', { vars: 'all', args: 'after-used', ignoreRestSiblings: false }],
    "space-before-function-paren": ["warn", {
      "anonymous": "always",
      "named": "never",
      "asyncArrow": "always"
    }],
    // TODO: fix all warnings and change rules back to "error"
    'ember/avoid-leaking-state-in-ember-objects': ['warn'],
    'ember/no-attrs-in-components': ['warn'],
    'prettier/prettier': ['warn']
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
