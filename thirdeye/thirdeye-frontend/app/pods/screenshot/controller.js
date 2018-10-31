import { alias } from '@ember/object/computed';
import { computed } from '@ember/object';
import Controller from '@ember/controller';
import moment from 'moment';

export default Controller.extend({
  // Default Legend text and color
  legendText: {
    dotted: {
      text: 'Expected',
      color: 'orange'
    },
    solid: {
      text: 'Current',
      color: 'blue'
    }
  },

  /**
   * Padding to be added to the graph
   */
  screenshotPadding: {
    left: 50,
    right: 100
  },

  // Displaying points for screenshot for n < 100
  point: computed('anomaly.dates', function() {
    const datesCount = this.get('anomaly.dates.length');
    return {
      show: datesCount <= 100
    };
  }),

  // Primary Anomaly details
  anomaly: alias('model.anomalyDetailsList.firstObject'),

  // Name of the metric for legend
  metricName: alias('anomaly.metric'),

  /** Data Massaging the anomaly for the graph component
   * @returns {Object}
   */
  primaryMetric: computed(
    'anomaly',
    'metricName',
    function() {
      const metricName = this.get('metricName');
      const anomaly = this.get('anomaly');
      return Object.assign(
        {},
        anomaly,
        {
          metricName,
          regions: [{
            start: anomaly.anomalyRegionStart,
            end: anomaly.anomalyRegionEnd
          }]
        }
      );
    }
  ),

  /**
   * Formats dates into ms unix
   * @returns {Array}
   */
  dates: computed('anomaly.dates.@each', function() {
    const dates = this.getWithDefault('anomaly.dates', []);
    const unixDates = dates.map((date) => moment(date).valueOf());

    return ['date', ...unixDates];
  }),

  current: alias('anomaly.currentValues'),
  expected: alias('anomaly.baselineValues'),

  /**
   * Data Massages current and expected values for the graph component
   * @returns {Array}
   */
  columns: computed(
    'current',
    'expected',
    'metricName',
    function() {
      const metricName = this.get('metricName');
      const currentColumn = [`${metricName}-current`, ...this.get('current')];
      const expectedColumn = [`${metricName}-expected`, ...this.get('expected')];
      return [currentColumn, expectedColumn];
    }
  )
});
