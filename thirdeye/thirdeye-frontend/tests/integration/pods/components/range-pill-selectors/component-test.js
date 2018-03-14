import $ from 'jquery';
import moment from 'moment';
import { module, test } from 'qunit';
import { run, later } from "@ember/runloop";
import { setupRenderingTest } from 'ember-qunit';
import { click, render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';

module('Integration | Component | range pill selectors', function(hooks) {
  setupRenderingTest(hooks);

  const PILL_CLASS = '.te-pill-selectors';
  const MAXTIME = 1520863199999; // tells us how much data is available for the selected metric
  const START_DATE = 1513151999999; // arbitrary start date in milliseconds
  const END_DATE = 1520873345292; // arbitrary end date in milliseconds
  const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
  const ACTIVE_DURATION = '1m'; // setting this date range option as default
  const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
  const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
  const TODAY = moment().startOf('day').add(1, 'days');
  const TWO_WEEKS_AGO = moment().subtract(13, 'days').startOf('day');
  const PRESET_RANGES = {
    'Today': [moment(), TODAY],
    'Last 2 weeks': [TWO_WEEKS_AGO, TODAY]
  };

  test('Confirming that range-pill-selector component renders and dates are selected properly', async function(assert) {

    // Prepare to verify action
    this.set('onRangeSelection', (actual) => {
      let expected = {
        isActive: true,
        value: "custom",
        name: "Custom",
        end: moment(TODAY).endOf('day').format(DISPLAY_DATE_FORMAT),
        start: moment(TWO_WEEKS_AGO).startOf('day').format(DISPLAY_DATE_FORMAT)
      };
      assert.deepEqual(actual, expected, 'selected start/end dates are passed to external action');
    });

    // Prepare props for component
    this.setProperties({
      maxTime: MAXTIME,
      uiDateFormat: UI_DATE_FORMAT,
      activeRangeEnd: moment(END_DATE).format(DISPLAY_DATE_FORMAT),
      activeRangeStart: moment(START_DATE).format(DISPLAY_DATE_FORMAT),
      timeRangeOptions: setUpTimeRangeOptions(['1m', '3m'], ACTIVE_DURATION),
      timePickerIncrement: TIME_PICKER_INCREMENT,
      predefinedRanges: PRESET_RANGES
    });

    // Rendering the component
    await render(hbs`
      {{range-pill-selectors
        title="Testing range pills"
        maxTime=maxTime
        uiDateFormat=uiDateFormat
        activeRangeEnd=activeRangeEnd
        activeRangeStart=activeRangeStart
        timeRangeOptions=timeRangeOptions
        timePickerIncrement=timePickerIncrement
        predefinedRanges=predefinedRanges
        selectAction=(action onRangeSelection)
      }}
    `);

    const $rangePill = this.$(`${PILL_CLASS}__item`);
    const $rangeTitle = this.$(`${PILL_CLASS}__title`);
    const $rangeInput = this.$('.daterangepicker-input');
    const $firstPill = this.$(`${PILL_CLASS}__item[data-value="${ACTIVE_DURATION}"]`);

    // Testing initial display of time range pills
    assert.equal(
      $firstPill.get(0).classList[1],
      `te-pill-selectors__item--active`,
      'Pill selected as default is highlighted');
    assert.equal(
      $rangeTitle.get(0).innerText,
      'Testing range pills',
      'Title of range pills is correct');
    assert.equal(
      $rangePill.get(0).innerText,
      'Last 30 Days',
      'Label of first pill is correct');
    assert.equal(
      $rangePill.get(1).innerText,
      '3 Months',
      'Label of 2nd pill is correct');
    assert.equal(
      $rangePill.get(2).innerText.includes('Custom'),
      true,
      'Label of 3nd pill is correct');
    assert.equal(
      $rangeInput.val(),
      `${moment(START_DATE).startOf('day').format(UI_DATE_FORMAT)} - ${moment(END_DATE).endOf('day').format(UI_DATE_FORMAT)}`,
      'Date range displayed in date-range-picker input is accurate');

    // Clicking to activate date-range-picker modal
    await run(() => $rangeInput.click());
    const $rangePresets = $('.daterangepicker .ranges ul li');

    // Brief confirmation that modal ranges are displaying properly
    assert.equal(
      $('.daterangepicker').get(0).style.display,
      'block',
      'Range picker modal is displayed');
    assert.equal(
      $rangePresets.get(0).innerText,
      'Today',
      'Range picker preset #1 is good');
    assert.equal(
      $rangePresets.get(1).innerText,
      'Last 2 weeks',
      'Range picker preset #2 is good');

    // Click on one of the preset ranges
    await run(() => $rangePresets.get(1).click());

    // Confirm that the custom pill gets highlighted and populated with selected dates
    assert.equal(
      this.$(`${PILL_CLASS}__item[data-value="custom"]`).get(0).classList[1],
      `te-pill-selectors__item--active`,
      'Selected pill is highlighted');
    assert.equal(
      this.$('.daterangepicker-input').val(),
      `${moment(TWO_WEEKS_AGO).startOf('day').format(UI_DATE_FORMAT)} - ${moment(TODAY).endOf('day').format(UI_DATE_FORMAT)}`,
      'Date range for selected custom preset is accurate');
  });
});
