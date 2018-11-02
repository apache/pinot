import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { visit, fillIn, click, currentURL, waitFor } from '@ember/test-helpers';
import { rootCauseConst as rcEl } from 'thirdeye-frontend/tests/utils/constants';
import $ from 'jquery';

module('Acceptance | rootcause', async function(hooks) {
  setupApplicationTest(hooks);

  test(`visiting /rootcause on metricId shows correct header title`, async assert => {
    await visit('/rootcause?metricId=1');

    assert.ok(
      $('.rootcause-header__major').get(0).value.includes('Investigation on pageViews'),
      'title is correct');
  });

  test(`visiting /rootcause on anomalyId shows correct header title`, async assert => {
    await visit('/rootcause?anomalyId=1');
    
    assert.ok(
      $('.rootcause-header__major').get(0).value.includes('Investigation on pageViews'),
      'title is correct');
  });

  test('empty state of rootcause page should have a placeholder and no tabs', async (assert) => {
    await visit('/rootcause');

    assert.equal(
      currentURL(),
      '/rootcause',
      'link is correct');
    assert.ok(
      $(rcEl.PLACEHOLDER).get(0),
      'placeholder exists'
    );
    assert.notOk(
      $(rcEl.TABS).get(0),
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
        $(rcEl.LABEL).get(0).innerText.trim(),
        'pageViews',
        'metric label is correct'
      );
      assert.equal(
        $(rcEl.SELECTED_METRIC).get(0).innerText.trim(),
        'pageViews',
        'selected metric is correct'
      );
    });

  test('visiting rootcause page and making changes to the title and comment should create a session with saved changes',
    async assert => {
      const header = 'My Session';
      const comment = 'Cause of anomaly is unknown';

      await visit('/rootcause?metricId=1');
      await fillIn(rcEl.HEADER, header);
      await fillIn(rcEl.COMMENT_TEXT, comment);
      await click(rcEl.SAVE_BTN);

      assert.equal(
        currentURL(),
        '/rootcause?sessionId=1',
        'link is correct');
      assert.equal(
        $(rcEl.HEADER).val(),
        'My Session',
        'session name is correct');
      assert.ok(
        $(rcEl.LAST_SAVED).get(0).innerText.trim().includes('Last saved by rootcauseuser'),
        'last saved information is correct');
      assert.equal(
        $(rcEl.COMMENT_TEXT).get(1).value,
        'Cause of anomaly is unknown',
        'comments are correct');
      assert.equal(
        $(rcEl.BASELINE).get(0).innerText.trim(),
        'WoW',
        'default baseline is correct');
    });

  test('Metrics, Dimensions, and Events tabs exist and should have correct information', async (assert) => {
    await visit('/rootcause?metricId=1');

    const $tabLinks = $(`${rcEl.TABS} .thirdeye-link`);

    assert.equal(
      $tabLinks.get(0).innerText.trim(),
      'Metrics',
      'default tab is correct');
    assert.ok(
      $(rcEl.METRICS_TABLE).get(0),
      'metrics table exist');

    // Click on Dimensions tab
    await click($tabLinks.get(1));

    assert.ok(
      $(rcEl.HEATMAP_DROPDOWN).get(0),
      'heatmap dropdown exists');
    assert.equal(
      $(rcEl.SELECTED_HEATMAP_MODE).get(4).innerText.trim(),
      'Change in Contribution',
      'default heatmap mode is correct');

    // Click on Events tab
    await click($tabLinks.get(2));
    assert.ok(
      $(rcEl.EVENTS_FILTER_BAR).get(0),
      'filter bar exists in events tab');
    assert.ok(
      $(rcEl.EVENTS_TABLE).get(0),
      'events table exists in events tab');
  });
});
