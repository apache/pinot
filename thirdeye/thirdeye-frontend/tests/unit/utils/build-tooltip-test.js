import BuildTooltip from 'thirdeye-frontend/utils/build-tooltip';
import { module, test } from 'qunit';

module('Unit | Utility | build-tooltip');

test('it builds', function(assert) {
  const hash = {
    entities: {},
    timeseries: {},
    hoverUrns: [''],
    hoverTimeStamp: {}
  };

  const tooltip = new BuildTooltip();
  const tooltipTemplate = tooltip.compute(hash).toString();

  assert.ok(
    tooltipTemplate.includes('<div class="te-tooltip">'),
    'The tooltip should be created correctly');
});
