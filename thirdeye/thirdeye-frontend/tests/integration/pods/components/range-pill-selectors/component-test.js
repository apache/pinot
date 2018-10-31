import $ from 'jquery';
import moment from 'moment';
import { module, test, skip } from 'qunit';
import { run } from "@ember/runloop";
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import wait from 'ember-test-helpers/wait';

module('Integration | Component | range pill selectors', function(hooks) {
  setupRenderingTest(hooks);

  const PILL_CLASS = '.range-pill-selectors';
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

  skip('Confirming that range-pill-selector component renders and dates are selected properly', async function(assert) {

    // Prepare to verify action
    this.set('onRangeSelection', (actual) => {
      let expected = {
        isActive: true,
        value: "custom",
        name: "Custom",
        end: moment(TODAY).format(DISPLAY_DATE_FORMAT),
        start: moment(TWO_WEEKS_AGO).format(DISPLAY_DATE_FORMAT)
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
      `range-pill-selectors__item--active`,
      'Pill selected as default is highlighted');
    assert.equal(
      $rangeTitle.get(0).innerText.trim(),
      'Testing range pills',
      'Title of range pills is correct');
    assert.equal(
      $rangePill.get(0).innerText.trim(),
      'Last 30 Days',
      'Label of first pill is correct');
    assert.equal(
      $rangePill.get(1).innerText.trim(),
      '3 Months',
      'Label of 2nd pill is correct');
    assert.equal(
      $rangePill.get(2).innerText.includes('Custom'),
      true,
      'Label of 3nd pill is correct');
    assert.equal(
      $rangeInput.val(),
      `${moment(START_DATE).format(UI_DATE_FORMAT)} - ${moment(END_DATE).format(UI_DATE_FORMAT)}`,
      'Date range displayed in date-range-picker input is accurate');

    // Clicking to activate date-range-picker modal
    await run(() => $rangeInput.click());
    const $rangePresets = $('.daterangepicker.show-calendar .ranges ul li');

    // Brief confirmation that modal ranges are displaying properly
    await wait();
    assert.equal(
      $('.daterangepicker.show-calendar').get(0).style.display,
      'block',
      'Range picker modal is displayed');
    await wait();
    assert.equal(
      $rangePresets.get(0).innerText.trim(),
      'Today',
      'Range picker preset #1 is good');
    await wait();
    assert.equal(
      $rangePresets.get(1).innerText.trim(),
      'Last 2 weeks',
      'Range picker preset #2 is good');

    // Click on one of the preset ranges
    await run(() => $rangePresets.get(1).click());
    await wait();
    // Confirm that the custom pill gets highlighted and populated with selected dates
    assert.equal(
      this.$(`${PILL_CLASS}__item[data-value="custom"]`).get(0).classList[1],
      `range-pill-selectors__item--active`,
      'Selected pill is highlighted');
    assert.equal(
      this.$('.daterangepicker-input').val(),
      `${moment(TWO_WEEKS_AGO).format(UI_DATE_FORMAT)} - ${moment(TODAY).format(UI_DATE_FORMAT)}`,
      'Date range for selected custom preset is accurate');
  });
});
