module.exports = {
  extends: 'eslint:recommended',
  parser: 'babel-eslint',
  env: {
    es6: true,
    browser: true,
    node: true
  },
  parserOptions: {
    ecmaVersion: 6,
    sourceType: 'module',
    ecmaFeatures: {
      experimentalObjectRestSpread: true
    }
  },
  plugins: [
  ],
  rules: {
    strict: 0,
    'indent': [2, 2],
    'prefer-const': 2,
    'no-unused-vars': 2,
    quotes: [2, 'single'],
    'eol-last': [0],
    'no-mixed-requires': [0],
    'no-underscore-dangle': [0],
    'keyword-spacing': 2,
    'semi': [2, 'never']
  }
}
