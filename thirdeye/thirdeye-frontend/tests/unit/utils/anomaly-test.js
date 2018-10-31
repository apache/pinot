import { module, test } from 'qunit';
import { getFormatedDuration } from 'thirdeye-frontend/utils/anomaly';

module('Unit | Utility | anomaly', function() {
  test('it returns a formated duration time correctly', function(assert) {
    assert.equal(getFormatedDuration(1491804013000, 1491890413000), '1 day', 'it returns correct duration ok');
  });

  test('it returns a non-zero duration time correctly', function(assert) {
    //We want to display only non-zero duration values in our table
    assert.equal(getFormatedDuration(1491804013000, 1491804013000), '', 'it filters out non-zero duration ok');
  });
});
