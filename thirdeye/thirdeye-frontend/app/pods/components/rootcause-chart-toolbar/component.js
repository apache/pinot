import Component from '@ember/component';
import moment from 'moment';

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
  analysisRangeMax: moment().endOf('day'),

  analysisRangePredefined: {
    'Today': [moment(), moment().endOf('day')],
    'Last 3 days': [moment().subtract(2, 'days').startOf('day'), moment().endOf('day')],
    'Last 7 days': [moment().subtract(6, 'days').startOf('day'), moment().endOf('day')],
    'Last 14 days': [moment().subtract(13, 'days').startOf('day'), moment().endOf('day')],
    'Last 28 days': [moment().subtract(27, 'days').startOf('day'), moment().endOf('day')]
  },

  granularityOptions: ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS'],

  compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],

  timeseriesModeOptions: ['absolute', 'relative', 'log', 'split'],

  //
  // internal
  //
  analysisRangeStart: null, // 0

  analysisRangeEnd: null, // 0

  granularity: null, // ""

  compareMode: null, // ""

  didReceiveAttrs() {
    const { context } = this.getProperties('context');

    this.setProperties({
      analysisRangeStart: moment(context.analysisRange[0]),
      analysisRangeEnd: moment(context.analysisRange[1]),
      granularity: context.granularity,
      compareMode: context.compareMode
    });
  },

  actions: {
   onContext() {
     const { context, onContext, analysisRangeStart, analysisRangeEnd, granularity, compareMode } =
       this.getProperties('onContext', 'context', 'analysisRangeStart', 'analysisRangeEnd', 'granularity', 'compareMode');

     const newContext = Object.assign({}, context, {
       analysisRange: [moment(analysisRangeStart).valueOf(), moment(analysisRangeEnd).valueOf()],
       granularity,
       compareMode
     });

     onContext(newContext)
   },

   onChart() {
      const { onChart, timeseriesMode } = this.getProperties('onChart', 'timeseriesMode');
      onChart(timeseriesMode);
    },

    onAnalysisRange(start, end) {
      this.setProperties({ analysisRangeStart: start, analysisRangeEnd: end });
      this.send('onContext');
    },

    onGranularity(granularity) {
      this.setProperties({ granularity });
      this.send('onContext');
    },

    onCompareMode(compareMode) {
      this.setProperties({ compareMode });
      this.send('onContext');
    },

    onTimeseriesMode(timeseriesMode) {
      this.setProperties({ timeseriesMode });
      this.send('onChart');
    }
  }
});
