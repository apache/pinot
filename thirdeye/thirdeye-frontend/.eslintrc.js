module.exports = {
  globals: {
    server: true,
  },
  root: true,
  parserOptions: {
    ecmaVersion: 8,
    sourceType: 'module',
    ecmaFeatures: {
      'experimentalObjectRestSpread': true
    }
  },
  extends: 'eslint:recommended',
  env: {
    browser: true,
    es6: true
  },
  rules: {
    "eol-last": ["warn", "always"],
    "indent": ["warn", 2, { "SwitchCase": 1 }],
    "space-in-parens": ["warn", "never"],
    "no-trailing-spaces": ["warn"],
    "comma-dangle": ["warn", "never"],
    "comma-spacing": ["warn", { "before": false, "after": true }],
    "semi": ["warn", "always"],
    "comma-style": ["warn", "last"],
    "no-unused-vars": ["warn", { "vars": "all", "args": "after-used", "ignoreRestSiblings": false }]
  }
};
