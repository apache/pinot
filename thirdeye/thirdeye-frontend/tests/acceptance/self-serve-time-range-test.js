import $ from 'jquery';
import { module, test, skip } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { selfServeConst } from 'thirdeye-frontend/tests/utils/constants';
import { visit, click, currentURL } from '@ember/test-helpers';
import wait from 'ember-test-helpers/wait';

module('Acceptance | verify time range consistency', function(hooks) {
  setupApplicationTest(hooks);

  const defaultDurationLabel = '3 Months';
  const defaultDurationKey = '3m';
  let rangePickerCustomRange = '';

  skip(`going through alert page flow to test time range behavior`, async (assert) => {
    server.createList('alert', 2);
    await visit(`/manage/alert/1`);

    assert.ok(
      $(selfServeConst.RANGE_PILL_SELECTOR_ACTIVE).get(0).innerText.includes(defaultDurationLabel),
      'Default time range is active'
    );

    // Clicking to activate date-range-picker modal
    await click(selfServeConst.RANGE_PICKER_INPUT);

    // Click on one of the preset ranges
    await click($(selfServeConst.RANGE_PICKER_PRESETS).get(1));

    // Cache value of new custom time range
    rangePickerCustomRange = $(selfServeConst.RANGE_PICKER_INPUT).val();
    await wait(selfServeConst.RANGE_PILL_SELECTOR_ACTIVE);

    await wait();
    assert.ok(
      $(selfServeConst.RANGE_PILL_SELECTOR_ACTIVE).get(0).innerText.includes('Custom'),
      'Custom time range pill selected'
    );

    assert.ok(
      currentURL().includes('duration=custom'),
      'Custom time range is correctly set as URL query parameter'
    );

    // Navigate to tuning route to make sure time range is persisted
    await click($(selfServeConst.LINK_TUNE_ALERT).get(0));

    assert.equal(
      $(selfServeConst.RANGE_PICKER_INPUT).val(),
      rangePickerCustomRange,
      'Custom time range dates as selected are persisted to tuning sub-route'
    );

    // Navigate back to Alert Page to make sure time range is persisted
    await click($(selfServeConst.LINK_ALERT_PAGE).get(0));

    assert.equal(
      $(selfServeConst.RANGE_PICKER_INPUT).val(),
      rangePickerCustomRange,
      'Custom time range dates as selected are persisted to alert page'
    );

    // Navigate back to alert search/list page to test time range reset
    await click($(selfServeConst.NAV_MANAGE_ALERTS).get(0));

    // Click on any alert to confirm time range in Alert Page is now reset
    await click($(selfServeConst.RESULTS_LINK).get(1));

    assert.ok(
      $(selfServeConst.RANGE_PILL_SELECTOR_ACTIVE).get(0).innerText.includes(defaultDurationLabel),
      'Previous custom time rage has been reset to default'
    );

    assert.ok(
      currentURL().includes(`duration=${defaultDurationKey}`),
      'Default time range is correctly set as URL query parameter'
    );
  });
});
