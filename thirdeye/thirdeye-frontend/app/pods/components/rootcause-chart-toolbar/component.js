import Component from '@ember/component';
import {
  computed,
  get,
  set,
  getProperties,
  setProperties
} from '@ember/object';
import translate from 'thirdeye-frontend/utils/translate';
import { makeTime } from 'thirdeye-frontend/utils/rca-utils';

/**
 * Mapping between translated human-readable granularity values and values that are named on the backend
 * @type {Object}
 */
const granularityMapping = {
  '5 Minutes': '5_MINUTES',
  '15 Minutes': '15_MINUTES',
  '1 Hour': '1_HOURS',
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

  //
  // external (optional)
  //
  analysisRangeMax: computed('compareMode', function() {
    const compareMode = get(this, 'compareMode');
    if (compareMode === 'forecast') {
      return null;
    }
    return makeTime().add(1, 'day').startOf('day');
  }),

  analysisRangePredefined: computed('compareMode', function() {
    const compareMode = get(this, 'compareMode');
    let ranges = {
      'Today': [makeTime(), makeTime().startOf('day').add(1, 'days')],
      'Last 3 days': [makeTime().subtract(2, 'days').startOf('day'), makeTime().startOf('day').add(1, 'days')],
      'Last 7 days': [makeTime().subtract(6, 'days').startOf('day'), makeTime().startOf('day').add(1, 'days')],
      'Last 14 days': [makeTime().subtract(13, 'days').startOf('day'), makeTime().startOf('day').add(1, 'days')],
      'Last 28 days': [makeTime().subtract(27, 'days').startOf('day'), makeTime().startOf('day').add(1, 'days')]
    };
    let futureRanges = {}
    if (compareMode === 'forecast') {
      futureRanges = {
        'Tomorrow': [makeTime().startOf('day').add(1, 'days'), makeTime().startOf('day').add(2, 'days')],
        'Next 3 days': [makeTime().startOf('day'), makeTime().startOf('day').add(3, 'days')],
        'Next 7 days': [makeTime().startOf('day'), makeTime().startOf('day').add(7, 'days')],
        'Next 14 days': [makeTime().startOf('day'), makeTime().startOf('day').add(14, 'days')],
        'Next 28 days': [makeTime().startOf('day'), makeTime().startOf('day').add(28, 'days')]
      };
    }
    return {
      ...ranges,
      ...futureRanges
    };
  }),

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
      analysisRangeStart: makeTime(context.analysisRange[0]),
      analysisRangeEnd: makeTime(context.analysisRange[1]),
      granularity: context.granularity,
      compareMode: context.compareMode
    });
  },

  actions: {
    onContext() {
      const { context, onContext, analysisRangeStart, analysisRangeEnd, granularity, compareMode } =
        getProperties(this, 'onContext', 'context', 'analysisRangeStart', 'analysisRangeEnd', 'granularity', 'compareMode');

      const newContext = Object.assign({}, context, {
        analysisRange: [makeTime(analysisRangeStart).valueOf(), makeTime(analysisRangeEnd).valueOf()],
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
      end = makeTime(end).startOf('day').valueOf();
      start = makeTime(start).valueOf();
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
