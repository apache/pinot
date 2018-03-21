import $ from 'jquery';
import { module, test } from 'qunit';
import { isPresent } from "@ember/utils";
import { setupApplicationTest } from 'ember-qunit';
import { visit, fillIn, click, currentURL, triggerKeyEvent } from '@ember/test-helpers';
import { filters, dimensions, granularities } from 'thirdeye-frontend/mocks/metricPeripherals';
import { selectChoose, clickTrigger } from 'thirdeye-frontend/tests/helpers/ember-power-select';

module('Acceptance | create alert', function(hooks) {
  setupApplicationTest(hooks);

  const METRIC_SELECT = '#select-metric';
  const METRIC_INPUT = '.ember-power-select-search-input';
  const GRANULARITY_SELECT = '#select-granularity';
  const DIMENSION_SELECT = '#select-dimension';
  const PATTERN_SELECT = '#select-pattern';
  const FILTER_SELECT = '#select-filters';
  const SUBGROUP_SELECT = '#config-group';
  const INPUT_NAME = '#anomaly-form-function-name';
  const GRAPH_CONTAINER = '.te-graph-alert';
  const OPTION_LIST = '.ember-power-select-options';
  const OPTION_ITEM = 'li.ember-power-select-option';
  const SELECTED_ITEM = '.ember-power-select-selected-item';
  const APP_OPTIONS = '#anomaly-form-app-name';
  const SPINNER = '.spinner-display';
  const CONFIG_GROUP_ALERTS = '.te-form__custom-label';
  const CONFIG_BLOCK = '.te-form__section-config';
  const CONFIG_RECIPIENTS_INPUT = '#config-group-add-recipients';
  const EMAIL_WARNING = '.te-form__alert--warning';
  const SUBMIT_BUTTON = '.te-button--submit';
  const SELECTED_CONFIG_GROUP = 'test_alert_1';
  const SELECTED_METRIC = 'test_collection_1::test_metric_1';
  const GROUP_RECIPIENT = 'simba@disney.com';
  const PATTERN_OPTIONS = [
    'Higher or lower than expected',
    'Higher than expected',
    'Lower than expected'
  ];
  // Flatten filter object in order to easily compare it to the list of options rendered
  const FILTER_ARRAY = Object.values(filters).map(filterGroup => [...Object.values(filterGroup)]);

  test(`visiting alert creation page to test onboarding flow for self-serve`, async (assert) => {
    await visit(`/self-serve/create-alert`);

    // Initial state: fields and graph are disabled
    assert.equal(
      $(GRANULARITY_SELECT).attr('aria-disabled'),
      'true',
      'Granularity field (representative) is disabled until metric is selected'
    );
    assert.equal(
      $(GRAPH_CONTAINER).get(0).classList[1],
      'te-graph-alert--pending',
      'Graph placeholder is visible. Data is not yet loaded.'
    );

    // Select a metric
    await click(METRIC_SELECT);
    await fillIn(METRIC_INPUT, 'test');
    await click($(`${OPTION_ITEM}:contains(${SELECTED_METRIC})`).get(0));

    // Fields are now enabled with defaults and load correct options, graph is loaded
    assert.equal(
      $(GRANULARITY_SELECT).find(SELECTED_ITEM).get(0).innerText,
      '5_MINUTES',
      'granularity field (representative) is enabled after metric is selected'
    );
    assert.equal(
      $(GRAPH_CONTAINER).get(0).classList.length,
      1,
      'Graph placeholder is replaced.'
    );
    assert.equal(
      $(GRAPH_CONTAINER).find('svg').length,
      3,
      'Graph and legend svg elements are rendered.'
    );
    assert.equal(
      $(SPINNER).length,
      0,
      'Loading icon is removed.'
    );

    // Now, verify that our selectable options are correct
    await click(GRANULARITY_SELECT);
    assert.equal(
      Object.values($(OPTION_LIST).get(0).children).map(item => item.innerText).join(),
      granularities.join(),
      'Granularity options render, number and text of options is correct'
    );

    await click(DIMENSION_SELECT);
    assert.equal(
      Object.values($(OPTION_LIST).get(0).children).map(item => item.innerText).join(),
      dimensions.join(),
      'Dimension options render, number and text of options is correct'
    );

    await click(FILTER_SELECT);
    assert.equal(
      Object.values($(OPTION_ITEM)).map(item => item.innerText).filter(item => isPresent(item)),
      FILTER_ARRAY.join(),
      'Filter options render, number and text of options is correct'
    );

    await click(PATTERN_SELECT);
    assert.equal(
      Object.values($(OPTION_ITEM)).map(item => item.innerText).filter(item => isPresent(item)),
      PATTERN_OPTIONS.join(),
      'Pattern options render, number and text of options is correct'
    );

    assert.equal(
      $(SUBMIT_BUTTON)[0].disabled,
      true,
      'Submit button is still disabled'
    );

    // Now verify expected field conditional behavior
    await selectChoose(PATTERN_SELECT, 'Higher or lower than expected');
    await selectChoose(APP_OPTIONS, 'the-lion-king');
    assert.equal(
      $(INPUT_NAME).val(),
      'theLionKing_testMetric1_upDown_5Minutes',
      'Alert name autocomplete primer is working.'
    );

    await click(SUBGROUP_SELECT);
    assert.equal(
      $(OPTION_ITEM).get(0).innerText,
      SELECTED_CONFIG_GROUP,
      'The config group associated with the selected app is found in the group selection options.'
    );

    await selectChoose(SUBGROUP_SELECT, SELECTED_CONFIG_GROUP);
    assert.equal(
      $(`${CONFIG_GROUP_ALERTS} a`).get(0).innerText,
      `See all alerts monitored by: ${SELECTED_CONFIG_GROUP}`,
      'Custom accordion block with alert table for selected group appears'
    );
    assert.equal(
      $(CONFIG_BLOCK).find('.control-label').get(0).innerText.replace(/\r?\n?/g, ''),
      `Recipients in subscription group ${SELECTED_CONFIG_GROUP}:${GROUP_RECIPIENT}`,
      'Label and email for recipients is correctly rendered'
    );

    await fillIn(CONFIG_RECIPIENTS_INPUT, GROUP_RECIPIENT);
    await triggerKeyEvent(CONFIG_RECIPIENTS_INPUT, 'keyup', '8');
    assert.equal(
      $(EMAIL_WARNING).get(0).innerText,
      `Warning: ${GROUP_RECIPIENT} is already included in this group.`,
      'Duplicate email warning appears correctly.'
    );

    await fillIn(CONFIG_RECIPIENTS_INPUT, 'duane@therock.com');
    assert.equal(
      $(SUBMIT_BUTTON)[0].disabled,
      false,
      'Submit button is enabled'
    );
  });
});

