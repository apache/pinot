/**
 * Component for root cause dimensions algorithm table
 * @module components/rootcause-dimensions-algorithm
 * @property {Array} entities - library of currently loaded RCA entities (contains metric properties we depend on)
 * @property {String} metricUrn - URN of currently loaded metric
 * @property {String} metricId - current metric Id
 * @property {Set} filters - current applied filters
 * @property {Array} range - 'analysisRange' from current RCA context
 * @property {String} mode - 'compareMode' from current RCA context
 * @property {Array} selectedUrns - the list of currently selected and graphed metrics. We sync this with the table's 'isSelecte' row property.
 * @example
    {{rootcause-dimensions-algorithm
      entities=entities
      metricUrn=metricUrn
      metricId=metricId
      filters=filteredUrns
      range=context.anomalyRange
      mode=context.compareMode
      selectedUrns=selectedUrns
      onSelection=(action "onSelection")
    }}
 * @exports rootcause-dimensions-algorithm
 * @author smcclung
 */

import fetch from 'fetch';
import Component from '@ember/component';
import { isPresent, isEmpty } from '@ember/utils';
import { task } from 'ember-concurrency';
import { inject as service } from '@ember/service';
import { checkStatus, makeFilterString } from 'thirdeye-frontend/utils/utils';
import { selfServeApiGraph } from 'thirdeye-frontend/utils/api/self-serve';
import { get, set, computed, getProperties, setProperties } from '@ember/object';
import {
  toCurrentUrn,
  toBaselineUrn,
  hasPrefix,
  toWidthNumber,
  toBaselineRange,
  toFilters,
  hasExclusionFilters
} from 'thirdeye-frontend/utils/rca-utils';
import { groupedHeaders, baseColumns } from 'thirdeye-frontend/shared/dimensionAnalysisTableConfig';
import d3 from 'd3';

const EXTRA_WIDTH = 0;

/* eslint-disable ember/avoid-leaking-state-in-ember-objects */
export default Component.extend({
  classNames: ['rootcause-dimensions'],
  dimensionsApiService: service('services/api/dimensions'),

  /**
   * Incoming start and end dates (context.analysisRange) for current metric or entity being analyzed
   * @type {Array}
   */
  range: [],

  /**
   * The type of change used by the current metric (context.compareMode)
   * @type {String}
   */
  mode: '',

  /**
   * Incoming collection of loaded entities cached in RCA services
   * @type {Object}
   */
  entities: {},

  /**
   * Incoming metric-related properties
   */
  selectedUrns: '',
  metricId: null,
  metricUrn: '',
  filters: '',

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
   * Modal open/close flag
   * @type {Boolean}
   */
  openSettingsModal: false,

  /**
   * These are defaults we want the modal to open with
   * @type {Object}
   */
  customTableSettings: {
    depth: '3',
    dimensions: [],
    summarySize: 20,
    orderType: 'auto',
    oneSideError: 'false',
    enableSubTotals: true,
    excludedDimensions: []
  },

  /**
   * Flag to help with table refresh logic (override caching)
   * @type {Boolean}
   */
  isUserCustomizingRequest: false,

  /**
   * Concatenated settings string
   * @type {String}
   */
  previousSettings: '',

  /**
   * Dimension data for models-table
   * @type {Array}
   */
  dimensionsRawData: [],
  dimensionOptions: [],

  /**
   * Caches for state of previous request
   */
  previousDimensionValues: [], // previous row's dimension names array
  previousMetricSettings: '', // previous request's metric urn and settings string
  previousCustomSettings: '', // previous request's custom settings string

  /**
   * Override for table classes
   * @type {Object}
   */
  dimensionTableClasses: {
    table: 'rootcause-dimensions-table table-condensed',
    noDataCell: 'rootcause-dimensions-table__column--blank-cell'
  },

  /**
   * Labels to omit from URN when isolating filter keys
   * @type {Array}
   */
  baseUrnArr: ['thirdeye', 'metric'],

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
   * Set up initial states for custom table settings
   */
  init() {
    this._super(...arguments);
    this._resetSettings(['initialTableSettings', 'savedSettings'], 'customTableSettings');
    const { sessionTableSettings, sessionUserCustomized } = this.getProperties(
      'sessionTableSettings',
      'sessionUserCustomized'
    );
    if (sessionTableSettings && sessionUserCustomized) {
      set(this, 'customTableSettings', sessionTableSettings);
      this.send('onSave');
    } else {
      // send table settings to parent so session can store them
      this.get('sendTableSettings')(this.get('customTableSettings'), this.get('isUserCustomizingRequest'));
    }
  },

  /**
   * Trigger conditional data fetch method on changing context properties (metric, time range, etc)
   */
  didReceiveAttrs() {
    this._super(...arguments);
    this._fetchIfNewContext();
  },

  /**
   * Data for each column of dimensions table
   * @type Array - array of objects, each corresponding to a row in the table
   */
  dimensionTableData: computed('dimensionsRawData.length', 'selectedUrns', function () {
    const { dimensionsRawData, selectedUrns } = this.getProperties('dimensionsRawData', 'selectedUrns');
    const toFixedIfDecimal = (number) =>
      number % 1 !== 0 ? number.toFixed(2).toLocaleString() : number.toLocaleString();
    const dimensionNames = dimensionsRawData.dimensions || [];
    const dimensionRows = dimensionsRawData.responseRows || [];
    let newDimensionRows = [];

    // If "sub-totals" rows are needed for each dimension grouping...
    if (get(this, 'enableSubTotals')) {
      this._insertDimensionTotalRows(dimensionRows);
    }

    // Build new dimension array for display as table rows
    if (dimensionRows.length) {
      let totalCost = 0;
      dimensionRows.map((record) => (totalCost += record.cost));
      dimensionRows.forEach((record, index) => {
        let {
          dimensionArr, // Generate array of cell-specific objects for each dimension
          dimensionUrn // Generate URN for each record from dimension names/values
        } = this._generateDimensionMeta(dimensionNames, record);
        let nodeSize = (record.sizeFactor || 0) * 100;
        let cost = totalCost !== 0 ? (record.cost || 0) / totalCost : record.cost || 0;
        cost = cost * 100;
        // New records of template-ready data
        newDimensionRows.push({
          id: index + 1,
          dimensionUrn,
          dimensionArr,
          names: record.names,
          dimensions: dimensionNames,
          isSelected: selectedUrns.has(dimensionUrn),
          percentageChange: record.percentageChange,
          percentageChangeNum: parseFloat(record.percentageChange),
          nodeSize: `${nodeSize.toFixed(4)}%`,
          nodeSizeNum: parseFloat(nodeSize),
          cost: `${cost.toFixed(4)}%`,
          costNum: parseFloat(cost),
          baseline: `${toFixedIfDecimal(record.baselineValue) || 0}`,
          baselineNum: parseFloat(record.baselineValue),
          current: `${toFixedIfDecimal(record.currentValue) || 0}`,
          currentNum: parseFloat(record.currentValue),
          elementWidth: this._calculateContributionBarWidth(dimensionRows, record)
        });
      });
    }
    return newDimensionRows;
  }),

  hasExclusion: computed('metricUrn', function () {
    const metricUrn = get(this, 'metricUrn');
    return hasExclusionFilters(toFilters(metricUrn));
  }),

  /**
   * Builds the columns array, pushing incoming dimensions into the base columns
   * @type {Array} Array of column objects
   */
  dimensionTableColumns: computed('dimensionsRawData.length', function () {
    const dimensionsRawData = get(this, 'dimensionsRawData');
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
          propertyName: 'dimensionValue',
          dimensionCategory: dimension,
          title: dimension.capitalize(),
          component: 'custom/dimensions-table/dimension',
          className: `${tableBaseClass} ${tableBaseClass}--med-width ${tableBaseClass}--custom`
        });
      });
    }
    // Merge the dynamic columns with the preset ones for the complete table
    return dimensionNamesArr.length ? [...dimensionColumns, ...baseColumns] : [];
  }),

  /**
   * Builds the headers array dynamically, based on availability of dimension records
   * @type {Array} Array of grouped headers
   */
  dimensionTableHeaders: computed('dimensionsRawData.length', 'selectedUrns', 'overallChange', function () {
    const { overallChange, dimensionsRawData } = getProperties(this, 'overallChange', 'dimensionsRawData');
    const dimensionNames = dimensionsRawData.dimensions || [];
    const tableHeaders = dimensionNames ? groupedHeaders(dimensionNames.length, overallChange) : [];
    return tableHeaders;
  }),

  /**
   * Prepare new rows for table to display rolled up totals for sub-dimensions
   * TODO: Remove this prototype if not needed. Else, dry it out some more (smcclung)
   * @method  _insertDimensionTotalRows
   * @param {Array} dimensionRows - array of dimension records
   * @private
   */
  _insertDimensionTotalRows(dimensionRows) {
    // Get all unique parent dimension names
    const firstLevelGroups = [...new Set(dimensionRows.map((row) => row.names[0]))];
    firstLevelGroups.forEach((topGroupName) => {
      if (!topGroupName.toLowerCase().includes('all')) {
        // Insert a "totals" row for each major dimension value (across all subdimensions)
        let dimNames = [topGroupName, 'All', 'All'];
        let dimGroup = dimensionRows.filter((rec) => rec.names[0] === topGroupName);
        let whereToInsert = dimensionRows.findIndex((record) => record.names[0] === topGroupName);
        this._rollUpSubDimensionTotals(dimensionRows, dimGroup, whereToInsert, dimNames);
        // Insert a "totals" row for each child value (sub-dimensions)
        let secondLevelGroups = [...new Set(dimGroup.map((item) => item.names[1]))];
        secondLevelGroups.forEach((subGroupName) => {
          let dimNames = [topGroupName, subGroupName, 'All'];
          let subGroup = dimGroup.filter((rec) => rec.names[1] === subGroupName);
          let whereToInsert = dimensionRows.findIndex(
            (record) => record.names[0] === topGroupName && record.names[1] === subGroupName
          );
          this._rollUpSubDimensionTotals(dimensionRows, subGroup, whereToInsert, dimNames);
        });
      }
    });
  },

  /**
   * Adds up total contribution values by dimension group and sub-group, creating a row for "Total" or "All"
   * It does this by adding rows to our existing dimension array.
   * TODO: Remove this prototype if not needed. Else, dry it out some more (smcclung)
   * @method  _rollUpSubDimensionTotals
   * @param {Array} dimensionRows - array of dimension records
   * @param {Array} dimSubGroup - filtered set of dimension rows to add up
   * @param {Number} whereToInsert - index of dimension array at which we insert our new totals rows
   * @param {Array} names - breakdown of dimension/subdimension names for given row
   * @private
   */
  _rollUpSubDimensionTotals(dimensionRows, dimSubGroup, whereToInsert, names) {
    const recordToCopy = dimensionRows[whereToInsert]; // Placeholder for actual values
    const newRecordObj = {
      names,
      currentValue: dimSubGroup.map((row) => row.currentValue).reduce((total, amount) => total + amount),
      baselineValue: dimSubGroup.map((row) => row.baselineValue).reduce((total, amount) => total + amount),
      percentageChange: recordToCopy.percentageChange,
      cost: recordToCopy.cost
    };
    dimensionRows.splice(whereToInsert, 0, newRecordObj);
  },

  /**
   * Extracts the filter keys/values from the metric URN and formats it for the API
   * @method _stringifyFilterForApi
   * @returns {String} filters param for 'autoDimensionOrder' request
   * @private
   */
  _stringifyFilterForApi(metricUrn) {
    const baseUrnArr = get(this, 'baseUrnArr');
    const metricArr = metricUrn.length ? metricUrn.split(':') : ['0'];
    // Isolate filter keys/values from incoming metric URN
    const rawFilterStr = metricArr
      .filter((urnFragment) => {
        return isNaN(urnFragment) && !baseUrnArr.includes(urnFragment);
      })
      .join(';');
    // Construct API-ready filter string
    const finalFilterStr = makeFilterString(decodeURIComponent(rawFilterStr));
    return rawFilterStr.length ? finalFilterStr : '';
  },

  /**
   * Decides whether to fetch new table data and reload component
   * @method _fetchIfNewContext
   * @private
   */
  _fetchIfNewContext() {
    const {
      mode,
      range,
      entities,
      metricUrn,
      previousMetricSettings,
      previousCustomSettings,
      customTableSettings,
      hasExclusion
    } = this.getProperties(
      'mode',
      'range',
      'entities',
      'metricUrn',
      'previousMetricSettings',
      'previousCustomSettings',
      'customTableSettings',
      'hasExclusion'
    );

    const metricEntity = entities[metricUrn];
    let isUserCustomizingRequest = get(this, 'isUserCustomizingRequest');
    // Concatenate incoming settings for bulk comparison
    const newMetricSettings = `${metricUrn}:${range[0]}:${range[1]}:${mode}`;
    const newCustomSettings = Object.values(customTableSettings).join(':');
    // Compare current and incoming metric settings if there are previous settings
    const isSameMetricSettings = previousMetricSettings ? previousMetricSettings === newMetricSettings : true;
    // Compare current and incoming custom settings
    const isSameCustomSettings = previousCustomSettings === newCustomSettings;

    // Reset settings if metrics have changed
    if (!isSameMetricSettings) {
      isUserCustomizingRequest = true;
      this._resetSettings(['customTableSettings', 'savedSettings'], 'initialTableSettings');
      // send table settings to parent so session can store them
      this.get('sendTableSettings')(this.get('customTableSettings'), this.get('isUserCustomizingRequest'));
    }

    // Abort if metric with exclusion filters
    if (hasExclusion) {
      return;
    }

    // If we have new settings, and a metric to work with, we can trigger fetch/reload. Otherwise, do nothing
    if (metricEntity && (!isSameCustomSettings || isUserCustomizingRequest)) {
      const parsedMetric = metricEntity.label.split('::');
      const metricId = metricUrn.match(/^thirdeye:metric:(\d+)/)[1];
      const baselineRange = toBaselineRange(range, mode); // start/end is dependent on 'compareMode' (WoW, Wo2W, etc)
      const requestObj = {
        metricUrn: metricUrn,
        metric: parsedMetric[1],
        dataset: parsedMetric[0],
        currentStart: range[0],
        currentEnd: range[1],
        baselineStart: baselineRange[0],
        baselineEnd: baselineRange[1],
        summarySize: customTableSettings.summarySize,
        oneSideError: customTableSettings.oneSideError,
        depth: customTableSettings.depth,
        orderType: customTableSettings.orderType,
        filters: this._stringifyFilterForApi(metricUrn)
      };
      // Add dimensions/exclusions into query if not empty
      if (customTableSettings.dimensions.length) {
        Object.assign(requestObj, { dimensions: customTableSettings.dimensions.join(',') });
      }
      if (customTableSettings.excludedDimensions.length) {
        Object.assign(requestObj, { excludedDimensions: customTableSettings.excludedDimensions.join(',') });
      }
      // Fetch dimension algorithm data
      get(this, 'fetchDimensionAnalysisData').perform(requestObj);
      // Fetch dimension options to populate settings modal dropdown
      get(this, 'fetchDimensionOptions').perform(metricId);
      // Cache incoming settings and URNs
      setProperties(this, {
        previousMetricSettings: newMetricSettings,
        previousCustomSettings: newCustomSettings
      });
    }
  },

  /**
   * As we're doing this object clone more than once to reset table settings, here is a
   * helper function, called this way: set(this, 'savedSettings', Object.assign({}, customTableSettings));
   * @method  _resetSettings
   * @param {Array} targetObjectNames - array of one or more object names
   * @param {String} sourceObjName - name of object to copy properties from
   * @private
   */
  _resetSettings(targetObjectNames, sourceObjName) {
    const sourceObj = get(this, sourceObjName);
    targetObjectNames.forEach((targetObj) => {
      set(this, targetObj, Object.assign({}, sourceObj));
    });
  },

  /**
   * Calculates offsets to use in positioning contribution bars based on aggregated widths
   * @method  _calculateContributionBarWidth
   * @param {Array} dimensionRows - array of dimension records
   * @param {Array} record - single current record
   * @returns {Object} positive and negative offset widths
   * @private
   */
  _calculateContributionBarWidth(dimensionRows, record) {
    const overallChangeValues = dimensionRows.map((row) => (row.cost ? row.cost : 0));
    const allValuesPositive = overallChangeValues.every((val) => val >= 0);
    const allValuesNegative = overallChangeValues.every((val) => val < 0);
    const widthAdditivePositive = allValuesPositive ? EXTRA_WIDTH : 0;
    const widthAdditiveNegative = allValuesNegative ? EXTRA_WIDTH : 0;

    // Find the largest change value across all rows
    const maxChange = d3.max(
      dimensionRows.map((row) => {
        return Math.abs(row.cost ? row.cost : 0);
      })
    );

    // Generate a scale mapping the change value span to a specific range
    const widthScale = d3.scaleLinear().domain([0, maxChange]).range([0, 100]);

    // Get sign of percentageChange
    const percentageValue = record.percentageChange ? toWidthNumber(record.percentageChange) : 0;

    // Convert contribution value to a width based on our scale
    const contributionValue = record.cost ? record.cost : 0;
    const signCarrier = percentageValue * contributionValue;
    const widthPercent = Math.round(widthScale(Math.abs(contributionValue)));

    // These will be used to set our bar widths/classes in dimensions-table/change-bars component
    return {
      positive: signCarrier >= 0 ? `${widthPercent + widthAdditivePositive}%` : '0%',
      negative: signCarrier >= 0 ? '0%' : `${widthPercent + widthAdditiveNegative}%`
    };
  },

  /**
   * Builds an array of objects with enough data for the dynamic dimension table columns to
   * know how to render each cell. Based on this object 'dimensionArr', we also build a rich URN
   * containing all the dimensions present in a record in a format that the RCA page understands.
   * @method  _generateDimensionMeta
   * @param {Array} dimensionNames - array of dimension names from root of response object
   * @param {Array} record - single current record
   * @returns {Object} name/value object for dimensions, plus the new URN
   * @private
   */
  _generateDimensionMeta(dimensionNames, record) {
    // We cache the value of the previous row's dimension values for row grouping
    const previousDimensionValues = get(this, 'previousDimensionValues');
    // We want to display excluded dimensions with value '(ALL)-' and having 'otherDimensionValues' prop
    const otherValues = isPresent(record, 'otherDimensionValues') ? record.otherDimensionValues : null;

    // Array to help dimension column component decide what to render in each cell
    const dimensionArr = dimensionNames.map((name, index) => {
      let dimensionValue = record.names[index] || null;
      let isExclusionRecord = dimensionValue === '(ALL)-';
      let modifiedValue = isExclusionRecord ? 'Other' : dimensionValue;
      return {
        label: name,
        value: modifiedValue ? modifiedValue.replace('(ALL)', 'All') : '-',
        isHidden: dimensionValue === previousDimensionValues[index], // if its a repeated value, hide it
        otherValues: isExclusionRecord ? otherValues : null
      };
    });

    // Create a string version of dimension name/value pairs
    const encodedDimensions = isPresent(dimensionArr)
      ? dimensionArr.map((dObj) => encodeURIComponent(`${dObj.label}=${dObj.value}`)).join(':')
      : '';
    // Append dimensions string to metricUrn. This will be sent to the graph legend for display
    const dimensionUrn = `${get(this, 'metricUrn')}:${encodedDimensions}`;
    // Now save the current record names as 'previous'
    set(this, 'previousDimensionValues', record.names);

    return { dimensionArr, dimensionUrn };
  },

  actions: {
    /**
     * Handle submission of custom settings from settings modal
     */
    onSave() {
      setProperties(this, {
        openSettingsModal: false,
        isUserCustomizingRequest: true
      });
      // Cache saved state
      this._resetSettings(['savedSettings'], 'customTableSettings');
      // send table settings to parent so session can store them
      this.get('sendTableSettings')(this.get('customTableSettings'), this.get('isUserCustomizingRequest'));
      this._fetchIfNewContext();
    },

    /**
     * Handle custom settings modal cancel
     */
    onCancel() {
      this._resetSettings(['customTableSettings'], 'savedSettings');
      // send table settings to parent so session can store them
      this.get('sendTableSettings')(this.get('customTableSettings'), this.get('isUserCustomizingRequest'));
      set(this, 'openSettingsModal', false);
    },

    /**
     * Handle custom settings modal open
     */
    onClickDimensionOptions() {
      set(this, 'openSettingsModal', true);
    },

    /**
     * Triggered on row selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} eventObj
     */
    displayDataChanged(eventObj) {
      if (isEmpty(eventObj.selectedItems)) {
        return;
      }
      const onSelection = get(this, 'onSelection');
      const selectedRows = eventObj.selectedItems;
      if (!onSelection) {
        return;
      }
      const selectedRecord = selectedRows[0];
      const urn = selectedRecord.dimensionUrn;
      const state = !selectedRecord.isSelected;
      const updates = { [urn]: state };
      if (hasPrefix(urn, 'thirdeye:metric:')) {
        updates[toCurrentUrn(urn)] = state;
        updates[toBaselineUrn(urn)] = state;
      }
      onSelection(updates);
    }
  },

  /**
   * Concurrency task to return a list of dimension options for the selected metric
   * @method fetchDimensionOptions
   * @param {Object} dimensionObj - required params for query
   * @returns {Generator object}
   * @private
   */
  fetchDimensionOptions: task(function* (metricId) {
    const dimensionList = yield fetch(selfServeApiGraph.metricDimensions(metricId)).then(checkStatus);
    const filteredDimensionList = dimensionList.filter((item) => item.toLowerCase() !== 'all');
    set(this, 'dimensionOptions', filteredDimensionList);
  }).drop(),

  /**
   * Concurrency task to call for either cached or new dimension data from store
   * @method fetchDimensionAnalysisData
   * @param {Object} dimensionObj - required params for query
   * @returns {Generator object}
   * @private
   */
  fetchDimensionAnalysisData: task(function* (dimensionObj) {
    const dimensionsPayload = yield this.get('dimensionsApiService').queryDimensionsByMetric(dimensionObj);
    const ratio = dimensionsPayload.globalRatio;

    this.setProperties({
      dimensionsRawData: dimensionsPayload,
      cachedUrn: get(this, 'metricUrn'),
      isDimensionDataPresent: true,
      overallChange: ratio ? `${((ratio - 1) * 100).toFixed(2)}%` : 'N/A'
    });
  }).drop()
});
