import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

const PLACEHOLDER = '.rootcause-placeholder';
const TABS = '.rootcause-tabs';
const LABEL = '.rootcause-legend__label';
const SELECTED_METRIC = '.rootcause-select-metric-dimension';

moduleForAcceptance('Acceptance | rootcause');

test('empty state of rootcause page should have a placeholder and no tabs', async (assert) => {
  await visit('/rootcause');

  assert.equal(
    currentURL(),
    '/rootcause',
    'link is correct');
  assert.ok(
    find(PLACEHOLDER).get(0),
    'placeholder exists'
  );
  assert.notOk(
    find(TABS).get(0),
    'tabs do not exist'
  );
});

test(`visiting /rootcause with only a metric provided should have correct metric name selected by default and displayed
in the legend`, async assert => {
  await visit('/rootcause?metricId=1');

  assert.equal(
    currentURL(),
    '/rootcause?metricId=1',
    'link is correct');

  assert.equal(
    find(LABEL).get(0).innerText,
    'pageViews',
    'metric label is correct'
  );

  assert.equal(
    find(SELECTED_METRIC).get(0).innerText,
    'pageViews',
    'selected metric is correct'
  );
});
