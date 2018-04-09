import $ from 'jquery';
import moment from 'moment';
import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { selfServeConst, selfServeSettings, optionsToString } from 'thirdeye-frontend/tests/utils/constants';
import { visit, fillIn, click, currentURL, triggerKeyEvent, waitUntil } from '@ember/test-helpers';
import { filters, dimensions, granularities } from 'thirdeye-frontend/mocks/metricPeripherals';
import { selectChoose, clickTrigger } from 'thirdeye-frontend/tests/helpers/ember-power-select';

module('Acceptance | tune alert settings', function(hooks) {
  setupApplicationTest(hooks);

  const alertLinkTitle = 'test_function_1';
  const alertProps = [
    'Metric',
    'Dataset',
    'Filtered by',
    'Breakdown by',
    'Owner',
    'Application',
    'Anomaly Pattern',
    'Mailing List',
    'Granularity'
  ];

  test(`visiting alert page to test self-serve tuning flow`, async (assert) => {
    server.createList('alert', 5);
    await visit(`/manage/alerts`);
    const $targetAlertLink = $(`${selfServeConst.RESULTS_LINK}:contains(${alertLinkTitle})`);

    // Verify default search results
    assert.equal(
      $(selfServeConst.RESULTS_TITLE).get(0).innerText,
      'Alerts Found(5)',
      'Number of alerts displayed and title are correct.'
    );

    // Click into Alert Page for first listed alert
    await click($targetAlertLink.get(0));
    const alertPropLabels = Object.values($(selfServeConst.ALERT_PROPS_ITEM)).filter(y => y.nodeName ==='DIV').map(x => x.innerText);


    assert.ok(
      currentURL().includes(`/manage/alert/1/explore?duration=3m`),
      'Navigation to alert page succeeded'
    );

    // Verify default search results
    assert.ok(
      alertPropLabels.join() === alertProps.join(),
      'All needed labels are displayed in header for Alert Page'
    );

    // Change the default date range, confirm it is cached in tuning page
    await click($(selfServeConst.RANGE_PILL_SELECTOR_TRIGGER).get(0));
    await click($(selfServeConst.RANGE_PILL_PRESET_OPTION).get(0));
    const urlCustomTune = currentURL().replace('explore', 'tune');

    // Navigate to tuning page, verify time range options in URL are the same
    await click($(selfServeConst.LINK_TUNE_ALERT).get(0));

    assert.equal(
      currentURL(),
      urlCustomTune,
      'In transition to tuning page, the user-selected custom date range was persisted'
    );

  });
});
