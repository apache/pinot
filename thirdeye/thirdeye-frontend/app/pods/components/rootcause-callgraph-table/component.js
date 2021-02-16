import { computed, getProperties } from '@ember/object';
import Component from '@ember/component';
import { toFilters } from 'thirdeye-frontend/utils/rca-utils';
import CALLGRAPH_TABLE_COLUMNS from 'thirdeye-frontend/shared/callgraphTableColumns';
import _ from 'lodash';

export default Component.extend({
  classNames: ['rootcause-metrics'],

  /**
   * Columns for dimensions table
   * @type {Array}
   */
  callgraphTableColumns: CALLGRAPH_TABLE_COLUMNS,

  //
  // external properties
  //

  /**
   * Primary metric
   * @type {string}
   */
  metricUrn: null,

  /**
   * Edges cache
   * @type {object}
   */
  edges: null,

  //
  // internal properties
  //

  /**
   * Tracks presence of callgraph edges
   * @type {boolean}
   */
  hasEdges: computed('edges', function () {
    const { edges } = getProperties(this, 'edges');
    return !_.isEmpty(edges);
  }),

  /**
   * Fabrics analyzed
   * @type {array}
   */
  fabrics: computed('metricUrn', function () {
    const { metricUrn } = getProperties(this, 'metricUrn');
    if (_.isEmpty(metricUrn)) {
      return [];
    }

    const fabrics = toFilters(metricUrn)
      .filter((t) => t[0] === 'data_center')
      .map((t) => t[2]);
    if (_.isEmpty(fabrics)) {
      return ['all'];
    }

    return fabrics;
  }),

  /**
   * Page keys analyzed
   * @type {array}
   */
  pageKeys: computed('metricUrn', function () {
    const { metricUrn } = getProperties(this, 'metricUrn');
    if (_.isEmpty(metricUrn)) {
      return [];
    }

    const pageKeys = toFilters(metricUrn)
      .filter((t) => t[0] === 'page_key')
      .map((t) => t[2]);

    return pageKeys;
  }),

  /**
   * Tracks presence of page keys
   * @type {boolean}
   */
  hasPageKeys: computed('pageKeys', function () {
    const { pageKeys } = getProperties(this, 'pageKeys');
    return !_.isEmpty(pageKeys);
  }),

  /**
   * Data for metrics table
   * @type {Array}
   */
  callgraphTableData: computed('edges', function () {
    const { edges } = getProperties(this, 'edges');

    if (_.isEmpty(edges)) {
      return [];
    }

    return Object.keys(edges).map((edge) =>
      toFilters(edge).reduce((agg, t) => {
        agg[t[0]] = this._parse(t[2]);
        return agg;
      }, {})
    );
  }),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed({
    get() {
      return [];
    },
    set() {
      // ignore
    }
  }),

  /**
   * Parse values heuristically as string or float
   * @private
   */
  _parse(value) {
    const f = parseFloat(value);
    if (!Number.isNaN(f)) {
      if (f >= 100) {
        return Math.round(f);
      }
      return (f / 1000.0).toFixed(3);
    }
    return value;
  },

  actions: {
    /**
     * Triggered on cell selection, ignored.
     * @param {Object} e
     */
    displayDataChanged(/* e */) {
      // ignore
    }
  }
});
