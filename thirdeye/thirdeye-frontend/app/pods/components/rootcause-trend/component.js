import { computed } from '@ember/object';
import Component from '@ember/component';
import {
  toCurrentUrn,
  toBaselineUrn,
  hasPrefix,
  filterPrefix,
  toMetricLabel,
  isInverse,
  toColorDirection,
  makeSortable
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange } from 'thirdeye-frontend/utils/utils';
import moment from 'moment';
import _ from 'lodash';

const ROOTCAUSE_TREND_MAX_COLUMNS = 12;

export default Component.extend({
  classNames: ['rootcause-metrics'],

  //
  // external properties
  //

  /**
   * Entities cache
   * @type {object}
   */
  entities: null,

  /**
   * Metric aggregates
   * @type {object}
   */
  timeseries: null,

  /**
   * Investigation context
   * @type {object}
   */
  context: null,

  /**
   * User-selected urns
   * @type {Set}
   */
  selectedUrns: null,

  /**
   * Callback on metric selection
   * @type {function}
   */
  onSelection: null, // function (Set, state)

  /**
   * loading status for component
   * @type {boolean}
   */
  isLoading: false,

  //
  // internal properties
  //

  /**
   * User-specified start time (in dropdown)
   * @type {int}
   */
  desiredStartTime: null,

  /**
   * Actual start time for time table (from context or dropdown)
   * @type {int}
   */
  startTime: computed(
    'context',
    'desiredStartTime',
    function () {
      const { context, desiredStartTime } = this.getProperties('context', 'desiredStartTime');

      const startTime = desiredStartTime || context.analysisRange[0];
      if (startTime < context.analysisRange[0]
          || startTime >= context.analysisRange[1]) {
        return context.analysisRange[0];
      }

      return startTime;
    }
  ),

  /**
   * Start time formatted
   * @type {string}
   */
  startTimeFormatted: computed('startTime', function () {
    return this._formatTime(this.get('startTime'));
  }),

  /**
   * Start time options for dropdown
   */
  startTimeOptions: computed('availableBuckets', function () {
    const availableBuckets = this.get('availableBuckets');

    const options = [];
    for (let i = 0; i < availableBuckets.length; i += ROOTCAUSE_TREND_MAX_COLUMNS) {
      options.push(this._formatTime(moment(availableBuckets[i])));
    }

    return options;
  }),

  /**
   * Reverse lookup mapping for start time options for dropdown
   */
  startTimeOptionsMapping: computed('availableBuckets', function () {
    const availableBuckets = this.get('availableBuckets');

    const options = {};
    for (let i = 0; i < availableBuckets.length; i += ROOTCAUSE_TREND_MAX_COLUMNS) {
      options[this._formatTime(moment(availableBuckets[i]))] = availableBuckets[i];
    }

    return options;
  }),

  /**
   * Possible time buckets from analysis range
   * @type {int[]}
   */
  availableBuckets: computed('context', function () {
    const context = this.get('context');

    const buckets = [];
    const [stepSize, stepUnit] = context.granularity.split('_').map(s => s.toLowerCase());
    const limit = moment(context.analysisRange[1]);
    let time = moment(context.analysisRange[0]);
    while (time < limit) {
      buckets.push(time.valueOf());
      time = time.add(stepSize, stepUnit);
    }

    return buckets;
  }),

  /**
   * Actual time buckets for columns
   * @type {int[]}
   */
  buckets: computed(
    'availableBuckets',
    'startTime',
    function () {
      const { availableBuckets, startTime } = this.getProperties('availableBuckets', 'startTime');

      const startOffset = startTime || availableBuckets[0];
      const startIndex = availableBuckets.findIndex(t => t >= startOffset);

      return _.slice(availableBuckets, startIndex, startIndex + ROOTCAUSE_TREND_MAX_COLUMNS);
    }
  ),

  /**
   * Columns for trend table
   * @type {object[]}
   */
  columns: computed('buckets', function () {
    const buckets = this.get('buckets');

    const columns = [
      {
        template: 'custom/table-checkbox',
        className: 'metrics-table__column'
      }, {
        propertyName: 'label',
        title: 'Metric',
        className: 'metrics-table__column metrics-table__column--large'
      }
    ];

    buckets.forEach(t => {
      columns.push({
        propertyName: `${t}`,
        template: 'custom/trend-table-cell',
        title: this._formatTime(t),
        sortedBy: `${t}_raw`,
        disableFiltering: true,
        className: 'metrics-table__column metrics-table__column--small'
      });
    });

    columns.push({
      template: 'custom/rca-metric-links',
      propertyName: 'links',
      title: 'Links',
      disableFiltering: true,
      disableSorting: true,
      className: 'metrics-table__column metrics-table__column--small'
    });

    return columns;
  }),

  /**
   * Change values, per metric/row
   * @type {object}
   */
  changes: computed(
    'entities',
    'timeseries',
    'buckets',
    function () {
      const { entities, timeseries, buckets } =
        this.getProperties('entities', 'timeseries', 'buckets');

      const changes = {};
      const metricUrns = filterPrefix(Object.keys(entities), 'thirdeye:metric:');

      metricUrns.forEach(urn => {
        const currUrn = toCurrentUrn(urn);
        const baseUrn = toBaselineUrn(urn);
        changes[urn] = Array(buckets.length).fill(Number.NaN);

        if (!timeseries[currUrn] || !timeseries[baseUrn]) {
          return;
        }

        // NOTE: lookup table to tolerate missing baseline values
        const baseValueLookup = {};
        for (let i = 0; i < timeseries[baseUrn].timestamp.length; i++) {
          baseValueLookup[timeseries[baseUrn].timestamp[i]] = timeseries[baseUrn].value[i];
        }

        timeseries[currUrn].timestamp.forEach((t, i) => {
          const change = timeseries[currUrn].value[i] / (baseValueLookup[t] || Number.NaN) - 1;

          // TODO use O(logN) data structure (e.g. treemap)
          const index = buckets.findIndex(b => b >= t);
          if (index >= 0 && index < buckets.length) {
            changes[urn][index] = change;
          }
        });
      });

      return changes;
    }
  ),

  /**
   * Table data in rows
   * @type {object[]}
   */
  data: computed(
    'entities',
    'buckets',
    'changes',
    'links',
    'selectedUrns',
    function () {
      const { entities, buckets, changes, links, selectedUrns } =
        this.getProperties('entities', 'buckets', 'changes', 'links', 'selectedUrns');

      const metricUrns = filterPrefix(Object.keys(entities), 'thirdeye:metric:');

      const rows = metricUrns.map(urn => {
        const row = {
          urn,
          label: toMetricLabel(urn, entities),
          isSelected: selectedUrns.has(urn),
          links: links[urn]
        };

        buckets.forEach((t, i) => {
          const change = changes[urn][i];
          row[`${t}`] = {
            change: humanizeChange(change),
            direction: toColorDirection(change, isInverse(urn, entities))
          };
          row[`${t}_raw`] = makeSortable(change);
        });

        return row;
      });

      return rows;
    }
  ),

  /**
   * A mapping of each metric and its url(s)
   * @type {Object} - key is metric urn, and value is an array of objects, each object has a key of the url label,
   * and value as the url
   * @example
   * {
   *  thirdeye:metric:12345: [],
   *  thirdeye:metric:23456: [
   *    {urlLabel: url},
   *    {urlLabel: url}
   *  ]
   * }
   */
  links: computed('entities', function() {
    const entities = this.get('entities');
    let metricUrlMapping = {};

    filterPrefix(Object.keys(entities), 'thirdeye:metric:')
      .forEach(urn => {
        const attributes = entities[urn].attributes;
        const { externalUrls = [] } = attributes;
        let urlArr = [];

        // Add the list of urls for each url type
        externalUrls.forEach(urlLabel => {
          urlArr.push({
            [urlLabel]: attributes[urlLabel][0] // each type should only have 1 url
          });
        });

        // Map all the url lists to a metric urn
        metricUrlMapping[urn] = urlArr;
      });

    return metricUrlMapping;
  }),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed(
    'data',
    'selectedUrns',
    function () {
      const { data, selectedUrns } = this.getProperties('data', 'selectedUrns');
      return [...selectedUrns].filter(urn => data[urn]).map(urn => data[urn]);
    }
  ),

  /**
   * Helper to format time stamp specifically for trend table
   *
   * @param {int} t timestamp
   * @private
   */
  _formatTime(t) {
    return moment(t).format('MM/DD h:mm') + moment(t).format('a')[0];
  },

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      if (_.isEmpty(e.selectedItems)) { return; }

      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');

      if (!onSelection) { return; }

      const urn = e.selectedItems[0].urn;
      const state = !selectedUrns.has(urn);

      const updates = {[urn]: state};
      if (hasPrefix(urn, 'thirdeye:metric:')) {
        updates[toCurrentUrn(urn)] = state;
        updates[toBaselineUrn(urn)] = state;
      }

      onSelection(updates);
    },

    /**
     * Triggered by drop down on change of selection
     */
    startTimeChanged(timestamp) {
      const lookup = this.get('startTimeOptionsMapping');
      this.set('desiredStartTime', lookup[timestamp]);
    }
  }
});
