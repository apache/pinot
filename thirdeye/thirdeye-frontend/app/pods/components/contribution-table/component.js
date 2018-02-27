import { isArray } from '@ember/array';
import { computed } from '@ember/object';
import { later } from '@ember/runloop';
import Component from '@ember/component';
import _ from 'lodash';

const GRANULARITY_MAPPING = {
  DAYS: 'M/D',
  HOURS: 'M/D h a',
  MINUTES: 'M/D hh:mm a',
  '5_MINUTES': 'M/D hh:mm a'
};

/**
 * Helper Function that filters cells betwen 2 index
 * @param {Array} rows
 * @param {Number} startIndex
 * @param {Number} endIndex
 * @return {Object} Hash of the rows with ids as key
 */
const filterRow = (rows, startIndex, endIndex) => {
  if (!startIndex && !endIndex) {
    return rows;
  }

  const newRows = rows.map((row) => {
    const All = Object.assign({}, row.subDimensionContributionMap.All);

    const newAll = Object.keys(All).reduce((agg, key) => {
      agg[key] = _.slice(All[key], startIndex, endIndex);
      return agg;
    }, {});

    return Object.assign({}, row, { subDimensionContributionMap: {All: newAll}});
  });

  return newRows;
};

export default Component.extend({
  metrics: null,
  showDetails: false,
  granularity: 'DAYS',
  primaryMetric: [],
  relatedMetrics: [],
  dimensions: [],
  start: null,
  end: null,

  stopLoading: () => {},

  // This is needed so that a loading spinner appears for long rendering
  didRender(...args) {
    this._super(args);

    later(() => {
      this.attrs.stopLoading();
    });
  },

  /**
   * Determines the date format based on granularity
   */
  dateFormat: computed('granularity', function() {
    const granularity = this.get('granularity');
    return GRANULARITY_MAPPING[granularity];
  }),

  /**
   * Contribution data of the primary metric
   */
  primaryMetricRows: computed('primaryMetric', function() {
    const metrics = this.get('primaryMetric');

    return isArray(metrics) ? [...metrics] : [Object.assign({}, metrics)];
  }),

  /**
   * Contribution data of the related metrics
   */
  relatedMetricRows: computed('relatedMetrics', function() {
    const metrics = this.get('relatedMetrics');

    return isArray(metrics) ? [...metrics] : [Object.assign({}, metrics)];
  }),

  /**
   * Contribution data of the dimension
   */
  dimensionRows: computed('dimensions', function() {
    const dimensions = this.get('dimensions');

    return isArray(dimensions) ? [...dimensions] : [Object.assign({}, dimensions)];
  }),

  /**
   * Finds the index of the first date greater than or equal to start
   * @param {Array} dates Array of dates
   * @param {Number} start Start date in unix ms
   * @return {Number} The start Index
   */
  startIndex: computed('dates', 'start', function() {
    const dates = this.get('dates');
    const start = this.get('start');

    const startIndex = dates.findIndex((v) => v >= start);
    if (startIndex <= -1) {
      return dates.length;
    }

    return startIndex;
  }),

  /**
   * Finds the index of the first date larger than or end
   * @param {Array} dates Array of dates
   * @param {Number} end end date in unix ms
   * @return {Number} The end Index
   */
  endIndex: computed('dates', 'end', function() {
    const dates = this.get('dates');
    const end = this.get('end');

    const endIndex = dates.findIndex((v) => v > end);
    if (endIndex <= -1) {
      return dates.length;
    }

    return endIndex;
  }),

  /**
   * Filters the date to return only those in range
   */
  filteredDates: computed(
    'startIndex',
    'endIndex',
    'dates',
    function() {
      const start = this.get('startIndex') || 0;
      const end = this.get('endIndex')|| 0;

      const dates = this.get('dates');
      if (!(start && end)) {
        return dates;
      }
      return _.slice(dates, start, end);
    }
  ),

  filteredPrimaryMetricRows: computed(
    'startIndex',
    'endIndex',
    'primaryMetricRows',
    function() {
      const startIndex = this.get('startIndex') || 0;
      const endIndex = this.get('endIndex') || 0;
      const rows = this.get('primaryMetricRows');


      return filterRow(rows, startIndex, endIndex) || [];
    }
  ),

  filteredRelatedMetricRows: computed(
    'startIndex',
    'endIndex',
    'relatedMetricRows',
    function() {
      const startIndex = this.get('startIndex') || 0;
      const endIndex = this.get('endIndex') || 0;
      const rows = this.get('relatedMetricRows');


      return filterRow(rows, startIndex, endIndex) || [];
    }
  ),

  filteredDimensionRows: computed(
    'startIndex',
    'endIndex',
    'dimensionRows',
    function() {
      const startIndex = this.get('startIndex') || 0;
      const endIndex = this.get('endIndex') || 0;
      const dimensions = this.get('dimensionRows');
      const valueKeys = [
        'baselineValues',
        'cumulativeBaselineValues',
        'cumulativeCurrentValues',
        'cumulativePercentageChange',
        'currentValues',
        'percentageChange'
      ];

      if (!startIndex && !endIndex) {
        return dimensions;
      }

      return dimensions.map((dimension) => {
        const hash = {
          name: dimension.name
        };
        valueKeys.forEach((key) => {
          hash[key] = _.slice(dimension[key], startIndex, endIndex);
        });
        return hash;
      }) || [];
    }
  )
});
