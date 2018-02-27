import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

const PLACEHOLDER = '.rootcause-placeholder';
const TABS = '.rootcause-tabs';
const LABEL = '.rootcause-legend__label';
const SELECTED_METRIC = '.rootcause-select-metric-dimension';
const ROOTCAUSE_HEADER = 'rootcause-header';
const HEADER = `.${ROOTCAUSE_HEADER}__major`;
const LAST_SAVED = `.${ROOTCAUSE_HEADER}__last-updated-info`;
const COMMENT_TEXT = `.${ROOTCAUSE_HEADER}--textarea`;
const BASELINE = '#select-compare-mode';
const EXPAND_ANOMALY_BTN = '.rootcause-anomaly__icon a';
const ANOMALY_TITLE = '.rootcause-anomaly__title';
const ANOMALY_VALUE = '.rootcause-anomaly__props-value';
const ANOMALY_STATUS = '.ember-radio-button.checked';
const SAVE_BTN = '.te-button';
const METRICS_TABLE = '.rootcause-metrics';
const HEATMAP_DROPDOWN = '#select-heatmap-mode';
const SELECTED_HEATMAP_MODE = '.ember-power-select-selected-item';
const EVENTS_FILTER_BAR = '.filter-bar';
const EVENTS_TABLE = '.events-table';

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

test('visiting rootcause page and making changes to the title and comment should create a session with saved changes',
  async assert => {
    const header = 'My Session';
    const comment = 'Cause of anomaly is unknown';

    await visit('/rootcause');
    await fillIn(HEADER, header);
    await fillIn(COMMENT_TEXT, comment);
    await click(SAVE_BTN);

    assert.equal(
      currentURL(),
      '/rootcause?sessionId=1',
      'link is correct');
    assert.equal(
      find(HEADER).get(0).value,
      'My Session',
      'session name is correct');
    assert.ok(
      find(LAST_SAVED).get(0).innerText.includes('Last saved by rootcauseuser'),
      'last saved information is correct');
    assert.equal(
      find(COMMENT_TEXT).get(1).value,
      'Cause of anomaly is unknown',
      'comments are correct');
    assert.equal(
      find(BASELINE).get(0).innerText,
      'WoW',
      'default baseline is correct');
  });

test('visiting rootcause page with an anomaly should have correct anomaly information', async assert => {
  await visit('/rootcause?anomalyId=1');
  await click(EXPAND_ANOMALY_BTN);

  assert.equal(
    currentURL(),
    '/rootcause?anomalyId=1',
    'link is correct');
  assert.equal(
    find(ANOMALY_TITLE).get(0).innerText,
    'Anomaly #1 anomaly_label',
    'anomaly title is correct'
  );
  assert.equal(
    find(ANOMALY_VALUE).get(0).innerText,
    'pageViews',
    'metric name in anomaly card is correct'
  );
  assert.equal(
    find(ANOMALY_STATUS).get(0).innerText.trim(),
    'No (False Alarm)',
    'anomaly status is correct');
});

test('Metrics, Dimensions, and Events tabs exist and should have correct information', async assert => {
  await visit('/rootcause?metricId=1');

  assert.equal(
    find(`${TABS} a`).get(0).innerText,
    'Metrics',
    'default tab is correct');
  assert.ok(
    find(METRICS_TABLE).get(0),
    'metrics table exist');

  // Click on Dimensions tab
  await click(find(`${TABS} a`).get(1));

  assert.ok(
    find(HEATMAP_DROPDOWN).get(0),
    'heatmap dropdown exists');
  assert.equal(
    find(SELECTED_HEATMAP_MODE).get(4).innerText,
    'Change in Contribution',
    'default heatmap mode is correct');

  // Click on Events tab
  await click(find(`${TABS} a`).get(2));
  assert.ok(
    find(EVENTS_FILTER_BAR).get(0),
    'filter bar exists in events tab');
  assert.ok(
    find(EVENTS_TABLE).get(0),
    'events table exists in events tab');
});
