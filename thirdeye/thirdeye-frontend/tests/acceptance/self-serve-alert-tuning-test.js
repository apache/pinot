import $ from 'jquery';
import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { selfServeConst } from 'thirdeye-frontend/tests/utils/constants';
import { visit, fillIn, click, currentURL, triggerKeyEvent, waitUntil } from '@ember/test-helpers';
import { filters, dimensions, granularities } from 'thirdeye-frontend/mocks/metricPeripherals';
import { selectChoose, clickTrigger } from 'thirdeye-frontend/tests/helpers/ember-power-select';

module('Acceptance | tune alert settings', function(hooks) {
  setupApplicationTest(hooks);

  const alertLinkTitle = 'test_function_1';
  const alertProps = [
    'Metric',
    'Dataset',
    'Granularity',
    'Application',
    'Created By',
    'Filtered By',
    'Breakdown By',
    'Detection Type',
    'Subscription Group'
  ];

  test(`check whether alerts page shows correct number of alerts`, async (assert) => {
    server.createList('alert', 5);
    await visit(`/manage/alerts`);

    // Verify default search results
    assert.equal(
      $(selfServeConst.RESULTS_TITLE).get(0).innerText.trim(),
      'Alerts (5)',
      'Number of alerts displayed and title are correct.'
    );
  });

  /**
   * TODO: extend this test with the following flow:
   * - click "preview performance", verify expected alert performance results, verify anomaly table results
   * - click "reset", verify clean slate
   * - click preview on custom settings, verify expected alert performance results
   * - save a custom MTTD, verify banner
   * - change date time range
   * - navigate back to alert page, verify mttd % is consistent and time range is persisted
   * - click report missing anomaly, enter time range, verify success banner and closed modal
   */
});
