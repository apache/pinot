import Component from '@ember/component';
import { inject as service } from '@ember/service';
import { computed } from '@ember/object';
import { task, timeout } from 'ember-concurrency';
import { get, set, getProperties } from '@ember/object';
import { toBaselineRange } from 'thirdeye-frontend/utils/rca-utils';
import { groupedHeaders, baseColumns } from 'thirdeye-frontend/shared/dimensionAnalysisTableConfig';

export default Component.extend({
  classNames: ['rootcause-dimensions-table'],
  dimensionsApiService: service('services/api/dimensions'),

  /**
   * Incoming cached state for rootcause view
   * Contains date-range and active metric settings
   * @type {Object}
   */
  context: {},

  /**
   * Incoming cached collection of loaded entities
   * @type {Object}
   */
  entities: {},

  /**
   * Incoming metric URN
   * @type {String}
   */
  selectedUrn: '',

  /**
   * Cached value to be inserted into table header
   * @type {String}
   */
  overallChange: 'NA',

  /**
   * Dimension data for models-table
   * @type {Array}
   */
  dimensionTableData: [],

  /**
   * Headers data for models-table
   * @type {Array}
   */
  dimensionTableHeaders: [],

  /**
   * Columns data for models-table
   * @type {Array}
   */
  dimensionTableColumns: [],

  /**
   * Boolean to prevent render pre-fetch
   * @type {Boolean}
   */
  isDimensionDataPresent: false,

  /**
   * Clears rendered table data
   * @returns {undefined}
   * @private
   */
  clearTable() {
    this.setProperties({
      dimensionTableData:[],
      dimensionTableHeaders:[],
      dimensionTableColumns:[],
      isDimensionDataPresent: false
    });
  },

  /**
   * Build request object for service query call each time we have fresh attributes if a 'metricEntity' is found
   */
  didReceiveAttrs() {
    this._super(...arguments);

    const { entities, selectedUrn, context } = this.getProperties('entities', 'selectedUrn', 'context');
    // Baseline start/end is dependent on 'compareMode' (WoW, Wo2W, etc)
    const baselineRange = toBaselineRange(context.analysisRange, context.compareMode);
    // If metric URN is found in entity list, proceed. Otherwise, we have no metadata to construct the call.
    const metricEntity = entities[selectedUrn];

    if (metricEntity) {
      const parsedMetric = metricEntity.label.split('::');
      const dimensionTableObj = {
        metric: parsedMetric[1],
        dataset: parsedMetric[0],
        currentStart: context.analysisRange[0],
        currentEnd: context.analysisRange[1],
        baselineStart: baselineRange[0],
        baselineEnd: baselineRange[1],
        summarySize: 20,
        oneSideError: false,
        depth: 3
      };
      // The table needs clearing at this point
      this.clearTable();
      // Load new data from either cache or new call
      get(this, 'loadDimensionAnalysisData').perform(dimensionTableObj);
    }
  },

  /**
   * Adds dimension data to dimension rows for table
   * @param {Object} dimensionsPayload - result from dimensions endpoint
   * @returns {Array} dimensionRows
   * @private
   */
  getDimensionTableData(dimensionsPayload) {
    const dimensionNames = dimensionsPayload.dimensions;
    const dimensionRows = dimensionsPayload.responseRows || [];
    let summaryRowIndex = 0; // row containing aggregated values

    if (dimensionRows.length) {
      // We are iterating over each row to make sure we have current-over-baseline and dimension data
      dimensionRows.forEach((record, index) => {
        set(record, 'cob', `${record.currentValue || 0} / ${record.baselineValue || 0}`);
        // One row should contain the aggregate data with overall change contribution
        if (record.names.every(name => name.includes('ALL'))) {
          set(this, 'overallChange', record.contributionToOverallChange);
          summaryRowIndex = index;
        }
        // Now, add a new property to each row for each available dimension
        dimensionNames.forEach((name, index) => {
          set(record, name.camelize(), record.names[index]);
        });
      });
    }

    // Remove the summary row from the array - not needed in table
    dimensionRows.splice(summaryRowIndex, 1);

    return dimensionRows || [];
  },

  /**
   * Builds the columns array, pushing incoming dimensions into the base columns
   * @param {Array} dimensionNamesArr - array containing dimension names
   * @returns {Array} combinedColumnsArr
   * @private
   */
  getDimensionTableColumns(dimensionNamesArr) {
    const tableBaseClass = 'advanced-dimensions-table__column';
    let dimensionArr = [];
    let combinedColumnsArr = [];

    if (dimensionNamesArr.length) {
      dimensionNamesArr.forEach((dimension) => {
        dimensionArr.push({
          propertyName: dimension.camelize(),
          title: dimension.capitalize(),
          className: `${tableBaseClass} ${tableBaseClass}--med-width`,
          disableFiltering: true
        });
      });
      combinedColumnsArr = [ ...dimensionArr, ...baseColumns ];
    }

    return combinedColumnsArr;
  },

  /**
   * Concurrency task to call for either cached or new dimension data from store
   * @param {Object} dimensionObj - required params for query
   * @returns {Generator object}
   * @private
   */
  loadDimensionAnalysisData: task(function * (dimensionObj) {
    const overallChange = get(this, 'overallChange');
    const dimensionsPayload = yield this.get('dimensionsApiService').queryDimensionsByMetric(dimensionObj);
    const dimensionNames = dimensionsPayload.dimensions || null;
    const dimensionTableHeaders = dimensionNames ? groupedHeaders(dimensionNames.length, overallChange) : [];
    const dimensionTableColumns = yield this.getDimensionTableColumns(dimensionNames);
    const dimensionTableData = yield this.getDimensionTableData(dimensionsPayload);
    this.setProperties({
      dimensionTableHeaders,
      dimensionTableColumns,
      dimensionTableData,
      isDimensionDataPresent: true
    });
    set(this, 'dimensionTableColumns', dimensionTableColumns);
  }).cancelOn('deactivate').restartable()

});
