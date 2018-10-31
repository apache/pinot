import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';

module('Integration | Component | anomaly id', function(hooks) {
  setupRenderingTest(hooks);

  const idSelector = '.anomaly-id__id';
  const nameSelector = '.anomaly-id__name';
  const currentValueSelector = '.anomaly-id__total';
  const changeRateSelector = '.anomaly-id__percent';
  const anomalyId = 20475;
  const anomalyCurrentValue = 10;
  const anomalyBaselineValue = 2;
  const anomaly = {
    anomalyIds: anomalyId,
    current: anomalyCurrentValue,
    baseline: anomalyBaselineValue,
    anomalyFunctionName: 'some-function-name'
  };
  const calculateChangeRate = (current, baseline) => {
    return (baseline === 0) ? 0 : floatToPercent((current - baseline) / baseline);
  };

  // Test for proper rendering of all Anomaly ID Block data
  test('Anomaly ID block: all meta properties render', async function(assert) {
    let changeRate = calculateChangeRate(anomaly.current, anomaly.baseline);

    // this.set('anomaly', anomaly);
    // this.set('anomalyChangeRate', changeRate);
    this.setProperties({ 'anomaly': anomaly, 'anomalyChangeRate': changeRate });
    await render(hbs`{{anomaly-id anomaly=anomaly}}`);

    assert.equal(this.$(idSelector).text().trim(), anomalyId, 'The anomaly id is correct and renders');
    assert.equal(this.$(nameSelector).text().trim(), anomaly.anomalyFunctionName, 'The anomaly name is correct and renders');
    assert.equal(this.$(currentValueSelector).text().trim(), anomaly.current, 'The current value is correct and renders');
    assert.equal(this.$(changeRateSelector).text().trim(), '(' + changeRate + '%)', 'The change rate is correct and renders');
  });

  // Now test for proper handling of a zero-value for baseline. In this case, percent change
  // cannot be calculated - we should return and display '0'
  test('Anomaly ID block: render correct change rate for zero baseline', async function(assert) {
    anomaly.baseline = 0;
    let changeRate = calculateChangeRate(anomaly.current, anomaly.baseline);

    // this.set('anomaly', anomaly);
    // this.set('anomalyChangeRate', changeRate);
    this.setProperties({ 'anomaly': anomaly, 'anomalyChangeRate': changeRate });
    await render(hbs`{{anomaly-id anomaly=anomaly}}`);

    assert.equal(this.$(changeRateSelector).text().trim(), '(' + changeRate + '%)', 'The change rate handles a zero baseline correctly');
  });
});
