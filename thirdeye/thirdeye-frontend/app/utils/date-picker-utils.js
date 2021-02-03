import moment from 'moment';

const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm';

const BASE_SPEC = {
  TIME_PICKER: true,
  TIME_PICKER_24_HOUR: true,
  TIME_PICKER_INCREMENT: 5,
  SHOW_CUSTOM_RANGE_LABEL: false,
  UI_DATE_FORMAT: 'MMM D, YYYY hh:mm a',
  SERVER_FORMAT: 'YYYY-MM-DD HH:mm'
};

const PREDEFINED_RANGES = {
  'Last 48 Hours': [moment().subtract(48, 'hour').startOf('hour'), moment().startOf('hour')],
  'Last Week': [moment().subtract(1, 'week').startOf('day'), moment().startOf('day')],
  'Last 30 Days': [moment().subtract(1, 'month').startOf('day'), moment().startOf('day')]
};

/**
 * Convert the date passed in milliseconds to string format ("YYYY-MM-DD HH:mm")
 *
 * @param {Number} date
 *   Date in milliseconds format
 *
 * @return {String}
 *   The date in "YYYY-MM-DD HH:mm" format
 */
const getInitialDate = (date) => {
  return moment(date).format(DISPLAY_DATE_FORMAT);
};

/**
 * Get the specification to initialize the date-range-picker with.
 *
 * @param {Number} startDate
 *   Start date in milliseconds format
 * @param {Number} endDate
 *   End date in milliseconds format
 *
 * @return {Object}
 *   The specification to initialize the date-range-picker
 */
export const getDatePickerSpec = (startDate, endDate) => {
  return {
    ...BASE_SPEC,
    PREDEFINED_RANGES: { ...PREDEFINED_RANGES },
    RANGE_START: getInitialDate(startDate),
    RANGE_END: getInitialDate(endDate)
  };
};
