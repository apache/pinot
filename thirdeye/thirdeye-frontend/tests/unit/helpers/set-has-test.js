
import { setHas } from 'thirdeye-frontend/helpers/set-has';
import { module, test } from 'qunit';

module('Unit | Helper | set has');

const testCases = ['alex', 'yves', 'thao'];
const testSet = new Set(testCases);

test('it works', function(assert) {
  const result = setHas([testSet, 'alex']);
  assert.ok(result);
});

test('it rejects', function(assert) {
  let result = setHas([testSet, 'Santa Claus']);
  assert.notOk(result);

  result = setHas([null, 'yves']);
  assert.notOk(result, 'passed argument must be a set');
});
