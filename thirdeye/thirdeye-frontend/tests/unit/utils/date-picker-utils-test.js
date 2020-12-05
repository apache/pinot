import { module, test } from "qunit";
import { getDatePickerSpec } from "thirdeye-frontend/utils/date-picker-utils";

module("Unit | Utility | Date picker utils", function() {
  test("it returns a the date picker spec correctly", function(assert) {
    const {
      TIME_PICKER,
      TIME_PICKER_24_HOUR,
      TIME_PICKER_INCREMENT,
      SHOW_CUSTOM_RANGE_LABEL,
      UI_DATE_FORMAT,
      SERVER_FORMAT,
      RANGE_START,
      RANGE_END,
      PREDEFINED_RANGES
    } = getDatePickerSpec(1602745200000, 1605427200000);

    const predefinedRangesKeys = Object.keys(PREDEFINED_RANGES);

    assert.equal(TIME_PICKER, true);
    assert.equal(TIME_PICKER_24_HOUR, true);
    assert.equal(TIME_PICKER_INCREMENT, 5);
    assert.equal(SHOW_CUSTOM_RANGE_LABEL, false);
    assert.equal(UI_DATE_FORMAT, "MMM D, YYYY hh:mm a");
    assert.equal(SERVER_FORMAT, "YYYY-MM-DD HH:mm");
    assert.ok(RANGE_START.includes("2020-10-15"));
    assert.ok(RANGE_END.includes("2020-11-15"));

    assert.equal(predefinedRangesKeys.length, 3);
    assert.deepEqual(predefinedRangesKeys, [
      "Last 48 Hours",
      "Last Week",
      "Last 30 Days"
    ]);
  });
});
