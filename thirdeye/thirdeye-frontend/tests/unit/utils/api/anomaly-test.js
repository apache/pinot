import { module, test } from 'qunit';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';

module('Unit | Utility | api/anomaly');

test('it returns anomaly data url correctly', function(assert) {
  assert.equal(getAnomalyDataUrl(0), '/dashboard/anomalies/view/0', 'it returns anomaly data url duration ok');
});
