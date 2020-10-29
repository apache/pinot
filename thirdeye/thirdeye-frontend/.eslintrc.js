module.exports = {
  globals: {
    server: true
  },
  root: true,
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module'
  },
  plugins: ['ember'],
  extends: ['eslint:recommended', 'plugin:ember/recommended'],
  env: {
    browser: true,
    es6: true
  },
  rules: {
    'eol-last': ['warn', 'always'],
    indent: ['warn', 2, { SwitchCase: 1 }],
    'space-in-parens': ['warn', 'never'],
    'no-trailing-spaces': ['warn'],
    'comma-dangle': ['warn', 'never'],
    'comma-spacing': ['warn', { before: false, after: true }],
    semi: ['warn', 'always'],
    'comma-style': ['warn', 'last'],
    'no-unused-vars': [
      'warn',
      { vars: 'all', args: 'after-used', ignoreRestSiblings: false },
    ],
    // TODO: fix all warnings and change rules back to 'error'
    'ember/avoid-leaking-state-in-ember-objects': ['warn'],
    'ember/no-attrs-in-components': ['warn'],
    'quotes': [2, 'single', { 'avoidEscape': true }]
  },
  overrides: [
    // node files
    {
      files: [
        'testem.js',
        'ember-cli-build.js',
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
        embertest: true
      }
    },
  ],
};
