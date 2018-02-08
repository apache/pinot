import Component from '@ember/component';
import moment from 'moment';
import { computed, get, set, getProperties, setProperties } from '@ember/object';
import translate from 'thirdeye-frontend/utils/translate';

/**
 * Mapping between translated human-readable granularity values and values that are named on the backend
 * @type {Object}
 */
const granularityMapping = {
  '5 Minutes': '5_MINUTES',
  '15 Minutes': '15_MINUTES',
  '1 Hour': '1_HOURS',
  '3 Hours': '3_HOURS',
  '1 Day': '1_DAYS'
};

export default Component.extend({

  //
  // external
  //
  onContext: null, // func (context)

  onChart: null, // func (timeseriesMode)

  context: null, // {}

  timeseriesMode: null, // ""

  //
  // external (optional)
  //
  analysisRangeMax: moment().startOf('day').add(1, 'days'),

  analysisRangePredefined: {
    'Today': [moment(), moment().startOf('day').add(1, 'days')],
    'Last 3 days': [moment().subtract(2, 'days').startOf('day'), moment().startOf('day').add(1, 'days')],
    'Last 7 days': [moment().subtract(6, 'days').startOf('day'), moment().startOf('day').add(1, 'days')],
    'Last 14 days': [moment().subtract(13, 'days').startOf('day'), moment().startOf('day').add(1, 'days')],
    'Last 28 days': [moment().subtract(27, 'days').startOf('day'), moment().startOf('day').add(1, 'days')]
  },

  /**
   * Mapping of granularity values translated from backend
   * @type {Object}
   */
  granularityMapping: granularityMapping,

  /**
   * Dropdown options for granularity
   * @type {String[]} - array of string values of options
   */
  granularityOptions: Object.keys(granularityMapping),

  compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],

  timeseriesModeOptions: ['absolute', 'relative', 'log', 'split'],

  //
  // internal
  //
  analysisRangeStart: null, // 0

  analysisRangeEnd: null, // 0

  granularity: null, // ""

  /**
   * Selected value in the granularity dropdown
   * @type {String}
   */
  selectedOption: computed('granularity', function() {
    return translate(this.granularityMapping, get(this, 'granularity'));
  }),

  compareMode: null, // ""

  didReceiveAttrs() {
    const { context } = getProperties(this, 'context');

    setProperties(this, {
      analysisRangeStart: moment(context.analysisRange[0]),
      analysisRangeEnd: moment(context.analysisRange[1]),
      granularity: context.granularity,
      compareMode: context.compareMode
    });
  },

  actions: {
    onContext() {
      const { context, onContext, analysisRangeStart, analysisRangeEnd, granularity, compareMode } =
        getProperties(this, 'onContext', 'context', 'analysisRangeStart', 'analysisRangeEnd', 'granularity', 'compareMode');

      const newContext = Object.assign({}, context, {
        analysisRange: [moment(analysisRangeStart).valueOf(), moment(analysisRangeEnd).valueOf()],
        granularity,
        compareMode
      });

      onContext(newContext);
    },

    onChart() {
      const { onChart, timeseriesMode } = getProperties(this, 'onChart', 'timeseriesMode');
      onChart(timeseriesMode);
    },

    onAnalysisRange(start, end) {
      setProperties(this, { analysisRangeStart: start, analysisRangeEnd: end });
      this.send('onContext');
    },

    /**
     * On selecting a granularity, sets the granularity and update the context
     * @param {String} granularity - selected granularity in the non-translated form (i.e. 1_HOURS)
     * @return {undefined}
     */
    onGranularity(granularity) {
      const translatedGranularity = this.granularityMapping[granularity];
      set(this, 'granularity', translatedGranularity);
      this.send('onContext');
    },

    onCompareMode(compareMode) {
      setProperties(this, { compareMode });
      this.send('onContext');
    },

    onTimeseriesMode(timeseriesMode) {
      setProperties(this, { timeseriesMode });
      this.send('onChart');
    }
  }
});
