import $ from 'jquery';
import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { selfServeConst, optionsToString } from 'thirdeye-frontend/tests/test-const';
import { visit, fillIn, click, currentURL, triggerKeyEvent } from '@ember/test-helpers';
import { filters, dimensions, granularities } from 'thirdeye-frontend/mocks/metricPeripherals';
import { selectChoose, clickTrigger } from 'thirdeye-frontend/tests/helpers/ember-power-select';

module('Acceptance | create alert', function(hooks) {
  setupApplicationTest(hooks);

  const selectedConfigGroup = 'test_alert_1';
  const selectedMetric = 'test_collection_1::test_metric_1';
  const groupRecipient = 'simba@disney.com';
  const newRecipient = 'duane@therock.com';
  const selectedApp = 'the-lion-king';

  // Flatten filter object in order to easily compare it to the list of options rendered
  const filterArray = Object.values(filters).map(filterGroup => [...Object.values(filterGroup)]);

  test(`visiting alert creation page to test onboarding flow for self-serve`, async (assert) => {
    await visit(`/self-serve/create-alert`);
    const $granularityDropdown = $(selfServeConst.GRANULARITY_SELECT);
    const $graphContainer = $(selfServeConst.GRAPH_CONTAINER);

    // Initial state: fields and graph are disabled
    assert.equal(
      $granularityDropdown.attr('aria-disabled'),
      'true',
      'Granularity field (representative) is disabled until metric is selected'
    );
    assert.equal(
      $graphContainer.get(0).classList[1],
      'te-graph-alert--pending',
      'Graph placeholder is visible. Data is not yet loaded.'
    );

    // Select a metric
    await click(selfServeConst.METRIC_SELECT);
    await fillIn(selfServeConst.METRIC_INPUT, 'test');
    await click($(`${selfServeConst.OPTION_ITEM}:contains(${selectedMetric})`).get(0));

    // Fields are now enabled with defaults and load correct options, graph is loaded
    assert.equal(
      $granularityDropdown.find(selfServeConst.SELECTED_ITEM).get(0).innerText,
      '5_MINUTES',
      'granularity field (representative) is enabled after metric is selected'
    );
    assert.equal(
      $graphContainer.get(0).classList.length,
      1,
      'Graph placeholder is replaced.'
    );
    assert.equal(
      $graphContainer.find('svg').length,
      3,
      'Graph and legend svg elements are rendered.'
    );
    assert.notOk(
      $(selfServeConst.SPINNER).length,
      'Loading icon is removed.'
    );

    // Now, verify that our selectable options are correct
    await click(selfServeConst.GRANULARITY_SELECT);
    assert.equal(
      optionsToString($(selfServeConst.OPTION_ITEM)),
      granularities.join(),
      'Granularity options render, number and text of options is correct'
    );

    await click(selfServeConst.DIMENSION_SELECT);
    assert.equal(
      optionsToString($(selfServeConst.OPTION_ITEM)),
      dimensions.join(),
      'Dimension options render, number and text of options is correct'
    );

    await click(selfServeConst.FILTER_SELECT);
    assert.equal(
      optionsToString($(selfServeConst.OPTION_ITEM)),
      filterArray.join(),
      'Filter options render, number and text of options is correct'
    );

    await click(selfServeConst.PATTERN_SELECT);
    assert.equal(
      optionsToString($(selfServeConst.OPTION_ITEM)),
      selfServeConst.PATTERN_OPTIONS.join(),
      'Pattern options render, number and text of options is correct'
    );
    assert.ok(
      $(selfServeConst.SUBMIT_BUTTON).get(0).disabled,
      'Submit button is still disabled'
    );

    // Now verify expected field conditional behavior
    await selectChoose(selfServeConst.PATTERN_SELECT, selfServeConst.PATTERN_OPTIONS[0]);
    await selectChoose(selfServeConst.APP_OPTIONS, selectedApp);
    assert.equal(
      $(selfServeConst.INPUT_NAME).val(),
      'theLionKing_testMetric1_upDown_5Minutes',
      'Alert name autocomplete primer is working.'
    );

    await click(selfServeConst.SUBGROUP_SELECT);
    assert.equal(
      $(selfServeConst.OPTION_ITEM).get(0).innerText,
      selectedConfigGroup,
      'The config group associated with the selected app is found in the group selection options.'
    );

    await selectChoose(selfServeConst.SUBGROUP_SELECT, selectedConfigGroup);
    assert.equal(
      $(selfServeConst.CONFIG_GROUP_ALERTS).get(0).innerText,
      `See all alerts monitored by: ${selectedConfigGroup}`,
      'Custom accordion block with alert table for selected group appears'
    );
    assert.equal(
      $(selfServeConst.CONFIG_BLOCK).find('.control-label').get(0).innerText.replace(/\r?\n?/g, ''),
      `Recipients in subscription group ${selectedConfigGroup}:${groupRecipient}`,
      'Label and email for recipients is correctly rendered'
    );

    await fillIn(selfServeConst.CONFIG_RECIPIENTS_INPUT, groupRecipient);
    await triggerKeyEvent(selfServeConst.CONFIG_RECIPIENTS_INPUT, 'keyup', '8');
    assert.equal(
      $(selfServeConst.EMAIL_WARNING).get(0).innerText,
      `Warning: ${groupRecipient} is already included in this group.`,
      'Duplicate email warning appears correctly.'
    );

    await fillIn(selfServeConst.CONFIG_RECIPIENTS_INPUT, newRecipient);
    assert.notOk(
      $(selfServeConst.SUBMIT_BUTTON).get(0).disabled,
      'Submit button is enabled'
    );
  });
});
