import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { module, test } from 'qunit';

module('Unit | Utility | float to percent', function() {
  test('it works', function(assert) {
    assert.equal(floatToPercent(0), 0.0);
    assert.equal(floatToPercent(0.55), 55.0);
    assert.equal(floatToPercent(0.123456), 12.35);
  });
});
