/**
 * Component for root cause dimensions table
 * @module components/rootcause-dimensions
 * @property {Array} entities - library of currently loaded RCA entities (contains metric properties we depend on)
 * @property {String} metricUrn - URN of currently loaded metric
 * @property {Object} context - a representation of the current cached state of the RCA page (we only care about its 'analysisRange' and 'compareMode' for now)
 * @property {Array} selectedUrns - the list of currently selected and graphed metrics. We sync this with the table's 'isSelecte' row property.
 * @property {Boolean} isLoading - loading state
 * @example
    {{rootcause-dimensions
      entities=entities
      metricUrn=metricUrn
      context=context
      selectedUrns=selectedUrns
      isLoading=(or isLoadingAggregates isLoadingScores)
      onSelection=(action "onSelection")
    }}
 * @exports rootcause-dimensions
 * @author smcclung
 */

import Component from '@ember/component';
import { once } from '@ember/runloop'
import { isPresent, isEmpty } from '@ember/utils';
import { task, timeout } from 'ember-concurrency';
import { inject as service } from '@ember/service';
import { computed, get, set, getProperties, setProperties } from '@ember/object';
import {
  toCurrentUrn,
  toBaselineUrn,
  hasPrefix,
  toWidthNumber,
  toBaselineRange
} from 'thirdeye-frontend/utils/rca-utils';
import {
  groupedHeaders,
  baseColumns
} from 'thirdeye-frontend/shared/dimensionAnalysisTableConfig';
import d3 from 'd3';

const EXTRA_WIDTH = 0;

export default Component.extend({
  classNames: ['rootcause-dimensions'],
  dimensionsApiService: service('services/api/dimensions'),

  /**
   * Incoming cached state for rootcause view
   * The context object is maintained by the RCA caching services. It defines the 'state' of the analysis view as
   * the user modifies options. This object will always contain start/end ranges, and active metric settings, which
   * this component needs to react to.
   * @type {Object}
   */
  context: {},

  /**
   * Incoming collection of loaded entities cached in RCA services
   * @type {Object}
   */
  entities: {},

  /**
   * Incoming metric URN
   * @type {String}
   */
  metricUrn: '',

  /**
   * Incoming metric URN
   * @type {String}
   */
  selectedUrns: '',

    /**
   * Existing metric URN
   * @type {String}
   */
  cachedUrn: '',

  /**
   * Callback on metric selection
   * @type {function}
   */
  onSelection: null, // function (Set, state)

  /**
   * Cached value to be inserted into table header
   * @type {String}
   */
  overallChange: 'NA',

  /**
   * Dimension data for models-table
   * @type {Array}
   */
  dimensionsRawData: [],

  /**
   * Override for table classes
   * @type {Object}
   */
  dimensionTableClasses: {
    table: 'rootcause-dimensions-table table-condensed',
    noDataCell: 'rootcause-dimensions-table__column--blank-cell'
  },

  /**
   * Boolean to prevent render pre-fetch
   * @type {Boolean}
   */
  isDimensionDataPresent: false,

  /**
   * Template for custom header row
   * @type {Boolean}
   */
  headerFilteringRowTemplate: 'custom/dimensions-table/header-row-filtering',

  /**
   * loading status for component
   * @type {boolean}
   */
  isLoading: false,

  didReceiveAttrs() {
    this._super(...arguments);

    // We're pulling in the entities list here because its the only way to extract the metric name and dataset
    const { entities, metricUrn, context } = this.getProperties('entities', 'metricUrn', 'context');
    // Baseline start/end is dependent on 'compareMode' (WoW, Wo2W, etc)
    const baselineRange = toBaselineRange(context.analysisRange, context.compareMode);
    // If metric URN is found in entity list, proceed. Otherwise, we have no metadata to construct the call.
    const metricEntity = entities[metricUrn];

    if (metricEntity) {
      const parsedMetric = metricEntity.label.split('::');
      once(() => {
        get(this, 'fetchDimensionAnalysisData').perform({
          metric: parsedMetric[1],
          dataset: parsedMetric[0],
          currentStart: context.analysisRange[0],
          currentEnd: context.analysisRange[1],
          baselineStart: baselineRange[0],
          baselineEnd: baselineRange[1],
          summarySize: 20,
          oneSideError: false,
          depth: 3
        });
      });
    }
  },

  /**
   * Data for dimensions table
   * @type Array - array of objects, each corresponding to a row in the table
   */
  dimensionTableData: computed(
    'dimensionsRawData.length',
    'selectedUrns',
    'metricUrn',
    function () {
      const { dimensionsRawData, selectedUrns, metricUrn } = this.getProperties('dimensionsRawData', 'selectedUrns', 'metricUrn');
      const dimensionNames = dimensionsRawData.dimensions || [];
      const dimensionRows = dimensionsRawData.responseRows || [];
      const toFixedIfDecimal = (number) => (number % 1 !== 0) ? number.toFixed(2) : number;
      let summaryRowIndex = 0; // row containing aggregated values
      let newDimensionRows = [];

      // We are iterating over each row to make sure we have current-over-baseline and dimension data
      if (dimensionRows.length) {
        dimensionRows.forEach((record, index) => {
          // Generate URN for each record from dimension names/values
          let { dimensionKeyVals, dimensionUrn } = this._buildDimensionalUrn(dimensionNames, record);
          let overallContribution = record.contributionToOverallChange;

          let newRow = {
            id: index - 1,
            dimensionUrn,
            ...dimensionKeyVals, // add a new property to each row for each available dimension
            names: record.names,
            dimensions: dimensionNames,
            percentageChange: record.percentageChange,
            contributionChange: record.contributionChange,
            contributionToOverallChange: overallContribution,
            isSelected: selectedUrns.has(dimensionUrn),
            cob: `${toFixedIfDecimal(record.currentValue) || 0} / ${toFixedIfDecimal(record.baselineValue) || 0}`,
            elementWidth: this._calculateContributionBarWidth(dimensionRows, record)
          };

          // Append to new table data array
          newDimensionRows.push(newRow);

          // One row should contain the aggregate data with overall change contribution
          if (record.names.every(name => name.includes('ALL'))) {
            set(this, 'overallChange', overallContribution);
            summaryRowIndex = index;
          }
        });

        // Remove the summary row from the array - not needed in table
        newDimensionRows.splice(summaryRowIndex, 1);
      }

      return newDimensionRows;
    }
  ),

  /**
   * Builds the columns array, pushing incoming dimensions into the base columns
   * @type {Array} Array of column objects
   */
  dimensionTableColumns: computed(
    'dimensionsRawData.length',
    'selectedUrns',
    function () {
      const { dimensionsRawData, selectedUrns } = this.getProperties('dimensionsRawData', 'selectedUrns');
      const dimensionNamesArr = dimensionsRawData.dimensions || [];
      const tableBaseClass = 'rootcause-dimensions-table__column';
      let dimensionColumns = [];

      if (dimensionNamesArr.length) {
        dimensionNamesArr.forEach((dimension, index) => {
          let isLastDimension = index === dimensionNamesArr.length - 1;
          dimensionColumns.push({
            disableSorting: true,
            isFirstColumn: index === 0,
            disableFiltering: isLastDimension, // currently overridden by headerFilteringRowTemplate
            propertyName: dimension.camelize(),
            title: dimension.capitalize(),
            isGrouped: !isLastDimension, // no label grouping logic on last dimension
            component: 'custom/dimensions-table/dimension',
            className: `${tableBaseClass} ${tableBaseClass}--med-width ${tableBaseClass}--custom`,
          });
        });
      }
      // Merge the dynamic columns with the preset ones for the complete table
      return dimensionNamesArr.length ? [ ...dimensionColumns, ...baseColumns ] : [];
    }
  ),


  /**
   * Builds the headers array dynamically, based on availability of dimension records
   * @type {Array} Array of grouped headers
   */
  dimensionTableHeaders: computed(
    'dimensionsRawData.length',
    'selectedUrns',
    'overallChange',
    function () {
      const { overallChange, dimensionsRawData }  = getProperties(this, 'overallChange', 'dimensionsRawData');
      const dimensionNames = dimensionsRawData.dimensions || [];
      const tableHeaders = dimensionNames ? groupedHeaders(dimensionNames.length, overallChange) : [];
      return tableHeaders;
    }
  ),

  /**
   * Calculates offsets to use in positioning contribution bars based on aggregated widths
   * @method  _calculateContributionBarWidth
   * @param {Array} dimensionRows - array of dimension records
   * @param {Array} record - single current record
   * @returns {Object} positive and negative offset widths
   * @private
   */
  _calculateContributionBarWidth(dimensionRows, record) {
    const overallChangeValues = dimensionRows.map(row => toWidthNumber(row.contributionToOverallChange));
    const allValuesPositive = overallChangeValues.every(val => val > 0);
    const allValuesNegative = overallChangeValues.every(val => val < 0);
    const widthAdditivePositive = allValuesPositive ? EXTRA_WIDTH : 0;
    const widthAdditiveNegative = allValuesNegative ? EXTRA_WIDTH : 0;

    // Find the largest change value (excluding the first 'totals' row)
    const maxChange = d3.max(dimensionRows.map((row) => {
      let isIndexRow = row.names.every(name => name.includes('ALL'));
      return isIndexRow ? 0 : Math.abs(toWidthNumber(row.contributionToOverallChange));
    }));

    // Generate a scale mapping the change value span to a specific range
    const widthScale = d3.scale.linear()
      .domain([0, maxChange])
      .range([0, 100]);

    // Convert contribution value to a width based on our scale
    const contributionValue = toWidthNumber(record.contributionToOverallChange);
    const widthPercent = Math.round(widthScale(Math.abs(contributionValue)));

    // These will be used to set our bar widths/classes in dimensions-table/change-bars component
    return {
      positive: (contributionValue > 0) ? `${widthPercent + widthAdditivePositive}%` : '0%',
      negative: (contributionValue > 0) ? '0%' : `${widthPercent + widthAdditiveNegative}%`
    }
  },

  /**
   * Builds a rich URN containing all the dimensions present in a record
   * @method  _buildDimensionalUrn
   * @param {Array} dimensionNames - array of dimension names from root of response object
   * @param {Array} record - single current record
   * @returns {Object} name/value object for dimensions and the URN
   * @private
   */
  _buildDimensionalUrn(dimensionNames, record) {
    // Object literal pulling in dimension names as keys
    const dimensionKeyVals = Object.assign(...dimensionNames.map((name, index) => {
      return { [name.camelize()]: record.names[index] || '-' };
    }));

    // Create a string version of dimension name/value pairs
    const encodedDimensions = isPresent(dimensionKeyVals) ? Object.keys(dimensionKeyVals).map((dName) => {
      return encodeURIComponent(`${dName}=${dimensionKeyVals[dName]}`);
    }).join(':') : '';

    const dimensionUrn = `${get(this, 'metricUrn')}:${encodedDimensions}`;
    return { dimensionKeyVals, dimensionUrn };
  },

  actions: {
    /**
     * Triggered on row selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} eventObj
     */
    displayDataChanged (eventObj) {
      if (isEmpty(eventObj.selectedItems)) { return; }
      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');
      const selectedRows = eventObj.selectedItems;
      if (!onSelection) { return; }
      const selectedRecord = selectedRows[0];
      const urn = selectedRecord.dimensionUrn;
      const state = !selectedRecord.isSelected;
      const updates = {[urn]: state};
      if (hasPrefix(urn, 'thirdeye:metric:')) {
        updates[toCurrentUrn(urn)] = state;
        updates[toBaselineUrn(urn)] = state;
      }
      onSelection(updates);
    }
  },

  /**
   * Concurrency task to call for either cached or new dimension data from store
   * @method fetchDimensionAnalysisData
   * @param {Object} dimensionObj - required params for query
   * @returns {Generator object}
   * @private
   */
  fetchDimensionAnalysisData: task(function * (dimensionObj) {
    const dimensionsPayload = yield this.get('dimensionsApiService').queryDimensionsByMetric(dimensionObj);
    const dimensionNames = dimensionsPayload.dimensions || [];

    this.setProperties({
      isLoading: false,
      dimensionsRawData: dimensionsPayload,
      cachedUrn: get(this, 'metricUrn'),
      isDimensionDataPresent: true
    });

  }).cancelOn('deactivate').drop()
});
