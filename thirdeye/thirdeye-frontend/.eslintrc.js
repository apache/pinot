module.exports = {
  "extends": "eslint:recommended",
  globals: {
    server: true,
  },
  root: true,
  parserOptions: {
    ecmaVersion: 8,
    sourceType: "module"
  },
  extends: 'eslint:recommended',
  env: {
    browser: true,
    es6: true
  },
  rules: {
    "eol-last": ["error", "always"],
    "indent": ["error", 2, { "SwitchCase": 1 }],
    "space-in-parens": ["error", "never"]
  }
};
