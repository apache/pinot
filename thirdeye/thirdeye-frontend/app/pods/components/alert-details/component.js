/**
 * This component displays the alert details. It would be used in places like Alert Details, and Preview pages/modules.
 * @module components/alert-details
 * @property {Object} alertYaml - the alert yaml
 * @property {boolean} disableYamlSave  - detect flag for yaml changes
 * @example
   {{#alert-details
     alertYaml=alertYaml
     disableYamlSave=disableYamlSave
   }}
     {{yield}}
   {{/alert-details}}
 * @exports alert-details
 */

import Component from '@ember/component';
import { computed, set, get, getProperties } from '@ember/object';
import { later } from '@ember/runloop';
import { checkStatus,
  humanizeFloat,
  postProps,
  stripNonFiniteValues,
  buildBounds } from 'thirdeye-frontend/utils/utils';
import { toastOptions } from 'thirdeye-frontend/utils/constants';
import { colorMapping, makeTime, toMetricLabel, extractTail } from 'thirdeye-frontend/utils/rca-utils';
import { getYamlPreviewAnomalies,
  getAnomaliesByAlertId,
  getBounds  } from 'thirdeye-frontend/utils/anomaly';
import { getValueFromYaml } from 'thirdeye-frontend/utils/yaml-tools';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import moment from 'moment';
import _ from 'lodash';
import config from 'thirdeye-frontend/config/environment';

const TABLE_DATE_FORMAT = 'MMM DD, hh:mm A'; // format for anomaly table
const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const ANOMALY_LEGEND_THRESHOLD = 20; // If number of anomalies is larger than this threshold, don't show the legend
const FORECAST_STRING = 'FORECAST';

export default Component.extend({
  anomaliesApiService: service('services/api/anomalies'),
  notifications: service('toast'),
  timeseries: null,
  analysisRange: [moment().subtract(2, 'day').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
  displayRange: [moment().subtract(2, 'day').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
  isPendingData: false,
  colorMapping: colorMapping,
  zoom: {
    enabled: true,
    rescale: true
  },
  point: {
    show: false,
    r: 5
  },
  errorTimeseries: null,
  metricUrn: null,
  metricUrnList: [],
  errorBaseline: null,
  compareMode: 'wo1w',
  baseline: null,
  showDetails: false,
  componentId: 'timeseries-chart',
  anomaliesOld: [],
  // flag for knowing the state of old anomalies, needed since there may be no anomalies
  anomaliesOldSet: false,
  anomaliesCurrent: [],
  // flag for knowing the state of old anomalies, needed since there may be no anomalies
  anomaliesCurrentSet: false,
  selectedBaseline: null,
  pageSize: 10,
  currentPage: 1,
  isPreviewMode: false,
  alertId: null,
  alertData: null,
  anomalyResponseNames: ['Not reviewed yet', 'Yes - unexpected', 'Expected temporary change', 'Expected permanent change', 'No change observed'],
  selectedDimension: null,
  isReportSuccess: false,
  isReportFailure: false,
  openReportModal: false,
  missingAnomalyProps: {},
  uniqueTimeSeries: [],
  selectedRule: {},
  isLoadingTimeSeries: false,
  granularity: null,
  alertYaml: null,
  //** overrides for ember-models-table defaults
  customMessages: {
    searchLabel: 'Search',
    searchPlaceholder: 'search by any column'
  },
  customClasses: {
    globalFilterWrapper: 'te-table-global-filter',
    outerTableWrapper: 'te-anomaly-table-wrapper',
    columnsDropdownWrapper: 'te-column-dropdown',
    table: 'table table-striped table-bordered table-condensed te-anomaly-table-body'
  },
  //**
  dimensionExploration: null,
  // cachedMetric holds the last metric of anomalies fetched, so that state can be reset for comparison if metric changes
  cachedMetric: null,
  getAnomaliesError:false, // stops the component from fetching more anomalies until user changes state
  detectionHealth: null, // result of call to detection/health/{id}, passed in by parent
  timeWindowSize: null, // passed in by parent, which retrieves from endpoint.  Do not set
  originalYaml: null, // passed by parent in Edit Alert Preview only. Do not set


  /**
   * This needs to be a computed variable until there is an endpoint for showing predicted with any metricurn
   * @type {Array}
   */
  baselineOptions: computed(
    'showRules',
    function() {
      const showRules = get(this, 'showRules');
      let options;
      if (showRules) {
        options = [
          { name: 'predicted', isActive: true},
          { name: 'wo1w', isActive: false},
          { name: 'wo2w', isActive: false},
          { name: 'wo3w', isActive: false},
          { name: 'wo4w', isActive: false},
          { name: 'mean4w', isActive: false},
          { name: 'median4w', isActive: false},
          { name: 'min4w', isActive: false},
          { name: 'max4w', isActive: false},
          { name: 'none', isActive: false}
        ];
      } else {
        options = [
          { name: 'wo1w', isActive: true},
          { name: 'wo2w', isActive: false},
          { name: 'wo3w', isActive: false},
          { name: 'wo4w', isActive: false},
          { name: 'mean4w', isActive: false},
          { name: 'median4w', isActive: false},
          { name: 'min4w', isActive: false},
          { name: 'max4w', isActive: false},
          { name: 'none', isActive: false}
        ];
      }
      return options;
    }
  ),

  /**
   * Flag for the stats box to show one or two values
   * @type {Boolean}
   */
  areTwoSetsOfAnomalies: computed(
    'anomaliesOldSet',
    'anomaliesCurrentSet',
    function() {
      return (this.get('anomaliesOldSet') && this.get('anomaliesCurrentSet'));
    }
  ),

  /**
   * Value to display in placeholder blob
   * @type {String}
   */
  blobMessage: computed(
    'getAnomaliesError',
    function() {
      let message = "Loading time series.";
      if (this.get('getAnomaliesError')) {
        message = "Error: can't get data.";
      }
      return message;
    }
  ),

  /**
   * Rules to display in rules dropdown
   * @type {Array}
   */
  ruleOptions: computed(
    'uniqueTimeSeries',
    function() {
      const uniqueTimeSeries = get(this, 'uniqueTimeSeries');
      if (uniqueTimeSeries) {
        return [...new Set(uniqueTimeSeries.map(series => series.detectorName))].map(detector => {
          const [ nameOnly, ruleType ] = detector.split(':');
          return {
            detectorName: detector,
            name: nameOnly,
            type: ruleType
          };
        });
      }
      return [];
    }
  ),

  /**
   * flag to differentiate preview loading and graph loading
   * @type {Boolean}
   */
  isPreviewLoading: computed(
    'isPreviewMode',
    '_getAnomalies.isIdle',
    function() {
      return (get(this, 'isPreviewMode') && !get(this, '_getAnomalies.isIdle'));
    }
  ),

  /**
   * flag for graph data loading
   * @type {Boolean}
   */
  isDataLoading: computed(
    'isLoadingTimeSeries',
    '_getAnomalies.isIdle',
    function() {
      return ((!get(this, '_getAnomalies.isIdle') || get(this, 'isLoadingTimeSeries')));
    }
  ),

  /**
   * flag for handling composite alert
   * @type {Boolean}
   */
  isComposite: computed(
    'alertData',
    function() {
      return (get(this, 'alertData') || {}).type === 'COMPOSITE_ALERT';
    }
  ),

  /**
   * flag to differentiate whether we show bounds and rules or not
   * @type {Boolean}
   */
  showRules: computed(
    'isPreviewMode',
    'granularity',
    'dimensionExploration',
    function() {
      const {
        isPreviewMode,
        granularity,
        dimensionExploration,
        isComposite
      } = this.getProperties('isPreviewMode', 'granularity', 'dimensionExploration', 'isComposite');
      return (isPreviewMode || (!isComposite && !dimensionExploration && ((granularity || '').includes('MINUTES') || (granularity || '').includes('DAYS'))));
    }
  ),

  /**
   * dimensions to display in dimensions dropdown
   * @type {Array}
   */
  dimensionOptions: computed(
    'metricUrnList',
    function() {
      const metricUrnList = get(this, 'metricUrnList');
      let options = [];
      metricUrnList.forEach(urn => {
        let dimensionUrn = toMetricLabel(extractTail(decodeURIComponent(urn)));
        dimensionUrn = dimensionUrn ? dimensionUrn : 'All Dimensions';
        options.push(dimensionUrn);
      });
      return options;
    }
  ),

  /**
   * Whether the alert has multiple dimensions
   * @type {Boolean}
   */
  alertHasDimensions: computed(
    'metricUrnList',
    function() {
      const metricUrnList = get(this, 'metricUrnList');
      return (metricUrnList.length > 1);
    }
  ),

  /**
   * Return state of anomalies and time series for updating state correctly
   * 1 - set to old (Alert Overview or Create Alert Preview)
   * 2 - set to new (Edit Alert Preview with old or Create Alert Preview w/o new)
   * 3 - shuffle then set to new (Create Alert Preview with 2 sets already) (not used for now)
   * 4 - get alert anomalies only - no time series (Edit Alert Preview w/o any anomalies loaded yet)
   * 5 - error getting anomalies
   * @type {Number}
   */
  stateOfAnomaliesAndTimeSeries: computed(
    'isPreviewMode',
    'anomaliesOldSet',
    'anomaliesCurrentSet',
    'isEditMode',
    'getAnomaliesError',
    function() {
      let state = 1;
      if (this.get('isPreviewMode')) {
        // Not Alert Preview
        if ((this.get('anomaliesCurrentSet')) || this.get('anomaliesOldSet')) {
          // At least one set of anomalies already loaded
          if (this.get('isEditMode')) {
            // replace current if Edit Alert Preview
            state = 2;
          } else {
            // Create Alert Preview  - shuffle
            state = 3;
          }
        } else if (this.get('isEditMode')) {
          // Edit Alert Preview w/o any anomalies
          state = 4;
        }
      }
      if (this.get('getAnomaliesError')) {
        state = 5;
      }
      return state;
    }
  ),

  /**
   * date-time-picker: indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: computed('alertData.windowUnit', function() {
    const rawGranularity = this.get('alertData.bucketUnit');
    const granularity = rawGranularity ? rawGranularity.toLowerCase() : '';

    switch(granularity) {
      case 'days':
        return 'MMM D, YYYY';
      case 'hours':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  disableRerunButton: computed(
    'alertYaml',
    'isLoading',
    'dataIsCurrent',
    function() {
      return (!get(this, 'alertYaml')|| get(this, 'isLoading') || get(this, 'dataIsCurrent'));
    }
  ),
  disablePreviewButton: computed(
    'alertYaml',
    '_getAnomalies.isIdle',
    function() {
      return (get(this, 'alertYaml') === null || !get(this, '_getAnomalies.isIdle'));
    }
  ),

  axis: computed(
    'analysisRange',
    'displayRange',
    'selectedBaseline',
    function () {
      const {
        analysisRange,
        displayRange,
        selectedBaseline
      } = this.getProperties('analysisRange', 'displayRange', 'selectedBaseline');

      const useRange = (selectedBaseline === 'predicted') ? displayRange : analysisRange;

      return {
        y: {
          show: true,
          tick: {
            format: function(d){return humanizeFloat(d);}
          }
        },
        y2: {
          show: false,
          min: 0,
          max: 1
        },
        x: {
          type: 'timeseries',
          show: true,
          min: useRange[0],
          max: useRange[1],
          tick: {
            fit: false,
            format: (d) => {
              const t = makeTime(d);
              if (t.valueOf() === t.clone().startOf('day').valueOf()) {
                return t.format('MMM D');
              }
              return t.format('h:mm a');
            }
          }
        }
      };
    }
  ),

  /**
   * Old anomalies to show in graph based on current dimension/rule combination
   * @type {Array}
   */
  filteredAnomaliesOld: computed(
    'anomaliesOld',
    'metricUrn',
    'selectedRule',
    'selectedDimension',
    'showRules',
    function() {
      const {
        metricUrn, anomaliesOld, selectedRule, showRules
      } = getProperties(this, 'metricUrn', 'anomaliesOld', 'selectedRule', 'showRules');
      return this._filterAnomalies(anomaliesOld, metricUrn, showRules, selectedRule);
    }
  ),



  /**
   * Old anomalies to show in graph based on current dimension/rule combination
   * @type {Array}
   */
  filteredAnomaliesCurrent: computed(
    'anomaliesCurrent',
    'metricUrn',
    'selectedRule',
    'selectedDimension',
    'showRules',
    function() {
      const {
        metricUrn, anomaliesCurrent, selectedRule, showRules
      } = getProperties(this, 'metricUrn', 'anomaliesCurrent', 'selectedRule', 'showRules');
      return this._filterAnomalies(anomaliesCurrent, metricUrn, showRules, selectedRule);
    }
  ),

  legend: computed(
    'numFilteredAnomalies',
    function() {
      if (get(this, 'numFilteredAnomalies') > ANOMALY_LEGEND_THRESHOLD) {
        return {
          show: false,
          position: 'right'
        };
      }
      return {
        show: true,
        position: 'right'
      };
    }
  ),

  numFilteredAnomalies: computed(
    'filteredAnomaliesOld.@each',
    'filteredAnomaliesCurrent.@each',
    function() {
      const filteredAnomalies = [...this.get('filteredAnomaliesOld'), ...this.get('filteredAnomaliesCurrent')];
      return filteredAnomalies.length;
    }
  ),

  isTrainingDisplayed: computed(
    'analysisRange',
    'displayRange',
    'selectedBaseline',
    function() {
      const {
        selectedRule, selectedBaseline
      } = this.getProperties('selectedRule', 'selectedBaseline');
      return (selectedRule.type === FORECAST_STRING && selectedBaseline === 'predicted');
    }
  ),

  trainingTooltipDateTimes: computed(
    'analysisRange',
    'displayRange',
    'isTrainingDisplayed',
    function() {
      const {
        analysisRange,
        displayRange
      } = this.getProperties('analysisRange', 'displayRange');
      const dateTimes = {
        fitStart: `${moment(displayRange[0]).format(TABLE_DATE_FORMAT)}`,
        fitEnd: `${moment(analysisRange[0]).format(TABLE_DATE_FORMAT)}`,
        forecastStart: `${moment(analysisRange[0]).format(TABLE_DATE_FORMAT)}`,
        forecastEnd: `${moment(analysisRange[1]).format(TABLE_DATE_FORMAT)}`
      };
      return dateTimes;
    }
  ),

  trainingMessage: computed(
    'analysisRange',
    'displayRange',
    'isTrainingDisplayed',
    function() {
      const {
        analysisRange, displayRange
      } = this.getProperties('analysisRange', 'displayRange');
      return `Training + Forecast (${moment(displayRange[0]).format(TABLE_DATE_FORMAT)} - ${moment(analysisRange[1]).format(TABLE_DATE_FORMAT)})`;
    }
  ),

  series: computed(
    'filteredAnomaliesOld.@each',
    'filteredAnomaliesCurrent.@each',
    'timeseries',
    'baseline',
    'selectedRule',
    'metricUrn',
    'isTrainingDisplayed',
    function () {
      const {
        filteredAnomaliesOld, filteredAnomaliesCurrent, timeseries, baseline,
        analysisRange, displayRange, showRules, isPreviewMode, isTrainingDisplayed
      } = getProperties(this, 'filteredAnomaliesOld', 'filteredAnomaliesCurrent',
        'timeseries', 'baseline', 'analysisRange', 'displayRange', 'showRules',
        'isPreviewMode', 'isTrainingDisplayed');

      const region = !isTrainingDisplayed ? {} :
        {
          timestamps: [displayRange[0], analysisRange[0]],
          values: [1, 1],
          type: 'region',
          axis: 'y2'
        };

      const series = {};
      series['training-region'] = region;
      // Should be displayed in Create Mode of Preview with one set of anomalies
      let anomaliesCurrentLabel = 'Current Settings Anomalies';
      let anomaliesOldLabel = 'Current Anomalies';
      // Should be displayed in Create Mode of Preview, if there are two sets of anomalies
      if (isPreviewMode && (this.get('stateOfAnomaliesAndTimeSeries') === 3)) {
        anomaliesOldLabel = 'Old Settings Anomalies';
        anomaliesCurrentLabel = 'New Settings Anomalies';
      // Should be displayed in Edit Mode of Preview ('real' anomalies saved in db)
      } else if (this.get('isEditMode')) {
        anomaliesOldLabel = 'Current Anomalies';
        anomaliesCurrentLabel = 'New Settings Anomalies';
      } else if (!isPreviewMode) {
        // Should be displayed in Alert Overview
        anomaliesCurrentLabel = 'Current Anomalies';
      }
      // The current time series has a different naming convention in Preview
      if (showRules) {
        if (timeseries && !_.isEmpty(timeseries.current)) {
          series['Current'] = {
            timestamps: timeseries.timestamp,
            values: stripNonFiniteValues(timeseries.current),
            type: 'line',
            color: 'screenshot-current'
          };
        }
      } else {
        if (timeseries && !_.isEmpty(timeseries.value)) {
          series['Current'] = {
            timestamps: timeseries.timestamp,
            values: stripNonFiniteValues(timeseries.value),
            type: 'line',
            color: 'screenshot-current'
          };
        }
      }

      if (baseline && !_.isEmpty(baseline.value)) {
        series['Selected Baseline'] = {
          timestamps: baseline.timestamp,
          values: stripNonFiniteValues(baseline.value),
          type: 'line',
          color: 'screenshot-predicted'
        };
      }

      // build upper and lower bounds, as relevant
      buildBounds(series, baseline, timeseries, showRules);

      // build set of anomalous values (newer of 2 sets of anomalies)
      this._mapAnomaliesToTimeSeries(
        filteredAnomaliesCurrent,
        timeseries,
        series,
        anomaliesCurrentLabel,
        'new-anomaly-edges',
        'red',
        true);

      // build set of previous anomalies
      this._mapAnomaliesToTimeSeries(
        filteredAnomaliesOld,
        timeseries,
        series,
        anomaliesOldLabel,
        'old-anomaly-edges',
        'grey',
        false);
      return series;
    }
  ),

  /**
   * formats anomalies for table
   * @method tableAnomalies
   * @return {Array}
   */
  tableAnomalies: computed(
    'anomaliesOld',
    'anomaliesCurrent',
    function() {
      const {
        anomaliesOld,
        anomaliesCurrent,
        analysisRange,
        stateOfAnomaliesAndTimeSeries
      } = this.getProperties('anomaliesOld', 'anomaliesCurrent', 'analysisRange', 'stateOfAnomaliesAndTimeSeries');
      let tableData = [];
      const humanizedObject = {
        queryDuration: analysisRange[1] - analysisRange[0],
        queryStart: analysisRange[0],
        queryEnd: analysisRange[1]
      };
      // we give the anomaly an arbitrary id for distinguishin in the frontend
      let fakeId = 0;
      if (anomaliesOld) {
        anomaliesOld.forEach(a => {
          const dimensionKeys = Object.keys(a.dimensions || {});
          const dimensionValues = dimensionKeys.map(d => a.dimensions[d]);
          const dimensionsString = [...dimensionKeys, ...dimensionValues].join();
          set(a, 'dimensionStr', dimensionsString);
          // 'settings' field only matters if column for settings shown
          set(a, 'settings', ((stateOfAnomaliesAndTimeSeries === 2) && this.get('isEditMode')) ? 'Current' : 'Old');
          // settingsNum is for sorting anomalies by 'new' vs 'old' regardless of label given
          set(a, 'settingsNum', 1);
          set(a, 'id', (!a.id) ? fakeId : a.id);
          set(a, 'startDateStr', this._formatAnomaly(a));
          set(a, 'current', a.avgCurrentVal);
          set(a, 'baseline', a.avgBaselineVal);
          set(a, 'rule', this.get('_formattedRule')(a));
          set(a, 'modifiedBy', this.get('_formattedModifiedBy')(a.feedback));
          set(a, 'updateTime', a.feedback ? a.feedback.updateTime : '');
          set(a, 'start', a.startTime);
          set(a, 'end', a.endTime);
          set(a, 'feedback', a.feedback ? a.feedback.feedbackType : a.statusClassification);
          set(a, 'severityLabel', a.severityLabel);
          if (a.feedback === 'NONE') {
            set(a, 'feedback', 'NO_FEEDBACK');
          }
          let tableRow = this.get('anomaliesApiService').getHumanizedEntity(a, humanizedObject);
          tableData.push(tableRow);
          ++fakeId;
        });
      }
      if (anomaliesCurrent) {
        anomaliesCurrent.forEach(a => {
          const dimensionKeys = Object.keys(a.dimensions || {});
          const dimensionValues = dimensionKeys.map(d => a.dimensions[d]);
          const dimensionsString = [...dimensionKeys, ...dimensionValues].join();
          set(a, 'dimensionStr', dimensionsString);
          // 'settings' field only matters if column for settings shown
          set(a, 'settings', 'New');
          // settingsNum is for sorting anomalies by 'new' vs 'old' regardless of label given
          set(a, 'settingsNum', 0);
          set(a, 'id', (!a.id) ? fakeId : a.id);
          set(a, 'startDateStr', this._formatAnomaly(a));
          set(a, 'current', a.avgCurrentVal);
          set(a, 'baseline', a.avgBaselineVal);
          set(a, 'rule', this.get('_formattedRule')(a));
          set(a, 'modifiedBy', this.get('_formattedModifiedBy')(a.feedback));
          set(a, 'updateTime', a.feedback ? a.feedback.updateTime : '');
          set(a, 'start', a.startTime);
          set(a, 'end', a.endTime);
          set(a, 'feedback', a.feedback ? a.feedback.feedbackType : a.statusClassification);
          set(a, 'severityLabel', a.severityLabel);

          if (a.feedback === 'NONE') {
            set(a, 'feedback', 'NO_FEEDBACK');
          }
          let tableRow = this.get('anomaliesApiService').getHumanizedEntity(a, humanizedObject);
          tableData.push(tableRow);
          ++fakeId;
        });
      }
      return tableData;
    }
  ),

  /**
   * flag for whether to show anomaly table
   * @method anomaliesAny
   * @return {Boolean}
   */
  anomaliesAny: computed(
    'tableAnomalies.@each',
    function() {
      return (this.get('tableAnomalies').length > 0);
    }
  ),

  /**
   * generates columns for anomaly table
   * @method columns
   * @return {Array}
   */
  columns: computed(
    'alertHasDimensions',
    'isPreviewMode',
    'stateOfAnomaliesAndTimeSeries',
    'isEditMode',
    'isComposite',
    function() {
      const {
        alertHasDimensions,
        isPreviewMode,
        stateOfAnomaliesAndTimeSeries,
        isEditMode,
        isComposite
      } = this.getProperties('alertHasDimensions', 'isPreviewMode',
        'stateOfAnomaliesAndTimeSeries', 'isEditMode', 'isComposite');
      const settingsColumn = ((isEditMode && stateOfAnomaliesAndTimeSeries === 2) ||
      stateOfAnomaliesAndTimeSeries === 3) ? [{
          title: 'Detection Settings',
          propertyName: 'settings',
          sortDirection: 'asc',
          sortedBy: 'settingsNum',
          sortPrecedence: 0 // lower number means higher precedence
        }] : [];
      const startColumn = [{
        template: 'custom/anomalies-table/start-duration',
        title: `Start / Duration (${moment().tz(config.timeZone).format('z')})`,
        propertyName: 'startDateStr',
        sortedBy: 'start',
        sortDirection: 'desc',
        sortPrecedence: 1 // lower number means higher precedence
      }];
      const dimensionColumn = (alertHasDimensions && !isComposite) ? [{
        template: 'custom/anomalies-table/dimensions-only',
        title: 'Dimensions',
        propertyName: 'dimensionStr'
      }] : [];
      const middleColumns = [{
        template: 'custom/anomalies-table/current-wow',
        title: 'Current / Predicted',
        propertyName: 'change'
      }, {
        component: 'custom/anomalies-table/rule',
        propertyName: 'rule',
        title: 'Rule'
      }, {
        component: 'custom/anomalies-table/severity-level',
        propertyName: 'severityLabel',
        title: 'Severity Level'
      }];
      const rightmostColumns = isPreviewMode ? [] : [{
        component: 'custom/anomalies-table/resolution',
        title: 'Feedback',
        propertyName: 'anomalyFeedback'
      }, {
        propertyName: 'modifiedBy',
        title: 'Modifier'
      }, {
        component: 'custom/anomalies-table/modify-time',
        propertyName: 'updateTime',
        title: 'Modify Time'
      }];
      const rcaColumn = (isComposite || isPreviewMode) ? [] : [{
        component: 'custom/anomalies-table/investigation-link',
        title: 'RCA',
        propertyName: 'id'
      }];
      return [...settingsColumn, ...startColumn, ...dimensionColumn,
        ...middleColumns, ...rightmostColumns, ...rcaColumn];
    }
  ),

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'anomaliesOld',
    'anomaliesCurrent',
    'stateOfAnomaliesAndTimeSeries',
    function() {
      const {
        anomaliesCurrent,
        anomaliesOld,
        isPreviewMode,
        isEditMode
      } = this.getProperties('anomaliesCurrent', 'anomaliesOld', 'isPreviewMode', 'isEditMode');
      let respondedAnomaliesCount = 0;
      let truePositives = 0;
      let falsePositives = 0;
      let falseNegatives = 0;
      let numberOfAnomalies = 0;
      let anomaliesToCalculate = anomaliesCurrent;
      // Only in the case of Edit Alert Preview will stats be based on anomaliesOld
      if (this.get('isEditMode')) {
        anomaliesToCalculate = anomaliesOld;
      }
      anomaliesToCalculate.forEach(function (anomaly) {
        numberOfAnomalies++;
        if(anomaly && anomaly.statusClassification) {
          const classification = anomaly.statusClassification;
          if (classification !== 'NONE') {
            respondedAnomaliesCount++;
            if (classification === 'TRUE_POSITIVE') {
              truePositives++;
            } else if (classification === 'FALSE_POSITIVE') {
              falsePositives++;
            } else if (classification === 'FALSE_NEGATIVE') {
              falseNegatives++;
            }
          }
        }
      });

      const totalAnomaliesCount = numberOfAnomalies;
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      let statsArray = [];
      if(!isPreviewMode || isEditMode) {
        const responseRate = respondedAnomaliesCount / totalAnomaliesCount;
        const precision = truePositives / (truePositives + falsePositives);
        const recall = truePositives / (truePositives + falseNegatives);
        const responseRateDescription = '% of anomalies that are reviewed';
        const precisionDescription = '% of all anomalies detected by the system that are true';
        const recallDescription = '% of all anomalies detected by the system';
        // old and new fields added for all blocks to allow for having comparison in all boxes
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit',
            anomaliesOld.length, anomaliesCurrent.length],
          ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent',
            floatToPercent(responseRate), floatToPercent(responseRate)],
          ['Precision', precisionDescription, floatToPercent(precision), 'percent',
            floatToPercent(precision), floatToPercent(precision)],
          ['Recall', recallDescription, floatToPercent(recall), 'percent',
            floatToPercent(recall), floatToPercent(recall)]
        ];
      } else {
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit',
            anomaliesOld.length, anomaliesCurrent.length]
        ];
      }
      return statsArray;
    }
  ),


  /**
   * Date types to display in the pills
   * @type {Object[]} - array of objects, each of which represents each date pill
   */
  pill: computed(
    'analysisRange', 'startDate', 'endDate', 'selectedRule',
    function() {
      const {
        analysisRange,
        selectedRule
      } = this.getProperties('analysisRange', 'selectedRule');
      const startDate = Number(analysisRange[0]);
      const endDate = Number(analysisRange[1]);
      const predefinedRanges = {
        'Last 48 Hours': [moment().subtract(48, 'hour').startOf('hour'), moment().startOf('hour')],
        'Last Week': [moment().subtract(1, 'week').startOf('day'), moment().startOf('day')],
        'Last 30 Days': [moment().subtract(1, 'month').startOf('day'), moment().startOf('day')],
        'Last 3 Months': [moment().subtract(3, 'month').startOf('day'), moment().startOf('day')]
      };
      if (selectedRule.type === FORECAST_STRING) {
        const futureRanges = {
          'Next 48 Hours': [moment().startOf('hour'), moment().add(48, 'hour').startOf('hour')],
          'Next Week': [moment().startOf('day'), moment().add(1, 'week').startOf('day')],
          'Next 30 Days': [moment().startOf('day'), moment().add(1, 'month').startOf('day')]
        };
        Object.assign(predefinedRanges, futureRanges);
      }
      return {
        uiDateFormat: UI_DATE_FORMAT,
        activeRangeStart: moment(startDate).format(DISPLAY_DATE_FORMAT),
        activeRangeEnd: moment(endDate).format(DISPLAY_DATE_FORMAT),
        timePickerIncrement: TIME_PICKER_INCREMENT,
        predefinedRanges
      };
    }
  ),

  _getAnomalies: task (function * (alertYaml) {//TODO: need to add to anomaly util - LH
    const {
      analysisRange,
      notifications,
      showRules,
      alertId,
      stateOfAnomaliesAndTimeSeries
    } = this.getProperties('analysisRange', 'notifications',
      'showRules', 'alertId', 'stateOfAnomaliesAndTimeSeries');
    //detection alert fetch
    const start = analysisRange[0];
    const end = analysisRange[1];
    let anomalies;
    let uniqueTimeSeries;
    let applicationAnomalies;
    let metricUrnList;
    let firstDimension;
    try {
      // case 4 is anomaliesOld for Edit Alert Preview, so we only need the real anomalies without time series
      if(showRules && stateOfAnomaliesAndTimeSeries !== 4){
        applicationAnomalies = (!this.get('isPreviewMode')) ? yield getBounds(alertId, start, end) : yield getYamlPreviewAnomalies(alertYaml, start, end, alertId);
        if (applicationAnomalies && applicationAnomalies.diagnostics && applicationAnomalies.diagnostics['0']) {
          metricUrnList = Object.keys(applicationAnomalies.diagnostics['0']);
          set(this, 'metricUrnList', metricUrnList);
          firstDimension = toMetricLabel(extractTail(decodeURIComponent(metricUrnList[0])));
          firstDimension = firstDimension ? firstDimension : 'All Dimensions';
          set(this, 'selectedDimension', firstDimension);
          if (applicationAnomalies.predictions && Array.isArray(applicationAnomalies.predictions) && (typeof applicationAnomalies.predictions[0] === 'object')){
            const detectorName = applicationAnomalies.predictions[0].detectorName;
            const [ nameOnly, ruleType ] = detectorName.split(':');
            const selectedRule = {
              detectorName,
              name: nameOnly,
              type: ruleType
            };
            set(this, 'selectedRule', selectedRule);
          }
          set(this, 'metricUrn', metricUrnList[0]);
        }
        // Alert Overview (should be real anomalies with ids)
        anomalies = ((stateOfAnomaliesAndTimeSeries === 1 && !this.get('isPreviewMode'))) ? yield getAnomaliesByAlertId(alertId, start, end) : applicationAnomalies.anomalies;
        uniqueTimeSeries = applicationAnomalies.predictions;
      } else {
        applicationAnomalies = yield getAnomaliesByAlertId(alertId, start, end);
        const metricUrnObj = {};
        if (applicationAnomalies) {
          applicationAnomalies.forEach(anomaly => {
            if (anomaly.metricUrn) {
              metricUrnObj[anomaly.metricUrn] = 1;
            }
          });
          metricUrnList = Object.keys(metricUrnObj);
          if (metricUrnList.length > 0) {
            firstDimension = toMetricLabel(extractTail(decodeURIComponent(metricUrnList[0])));
            firstDimension = firstDimension ? firstDimension : 'All Dimensions';
            this.setProperties({
              metricUrnList,
              selectedDimension: firstDimension,
              metricUrn: metricUrnList[0]
            });
          }
        }
        anomalies = applicationAnomalies;
      }
      set(this, 'cachedMetric', getValueFromYaml('metric', alertYaml, 'string'));
    } catch (error) {
      const previewErrorMsg = (error.body && typeof error.body === 'object') ? error.body.message : error.message;
      const previewErrorInfo = (error.body && typeof error.body === 'object') ? error.body['more-info'] : error['more-info'];
      const previewPrompt = this.get('isPreviewMode') ? ' Check warning above detection config.' : '';
      notifications.error(`Failed to get anomalies.${previewPrompt}`, 'Error', toastOptions);
      this.set('getAnomaliesError', true);
      if (this.get('isPreviewMode')) {
        this.get('sendPreviewError')({
          previewError: true,
          previewErrorMsg,
          previewErrorInfo
        });
      }
    }

    return {
      anomalies,
      uniqueTimeSeries
    };
  }).keepLatest(),

  init() {
    this._super(...arguments);
    const {
      granularity,
      isPreviewMode,
      dimensionExploration
    } = this.getProperties('granularity', 'isPreviewMode', 'dimensionExploration');
    let timeWindowSize = get(this, 'timeWindowSize');
    timeWindowSize = timeWindowSize ? timeWindowSize : 172800000; // 48 hours in milliseconds
    if (!isPreviewMode) {
      this.setProperties({
        analysisRange: [moment().subtract(timeWindowSize, 'milliseconds').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
        displayRange: [moment().subtract(timeWindowSize, 'milliseconds').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
        selectedDimension: 'Choose a dimension',
        // For now, we will only show predicted and bounds on daily and minutely metrics with no dimensions, for the Alert Overview page
        selectedBaseline: (!dimensionExploration && ((granularity || '').includes('MINUTES') || (granularity || '').includes('DAYS'))) ? 'predicted' : 'wo1w',
        // We distinguish these because it only needs the route's info on init.  After that, component manages state
        metricUrnList: this.get('metricUrnListRoute'),
        metricUrn: this.get('metricUrnRoute')
      });

      this._fetchAnomalies();
    } else {
      this.setProperties({
        analysisRange: [moment().subtract(timeWindowSize, 'milliseconds').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
        displayRange: [moment().subtract(timeWindowSize, 'milliseconds').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
        selectedBaseline: 'predicted'
      });
    }
    if (this.get('isEditMode')) {
      this.setProperties({
        // We distinguish these because it only needs the route's info on init.  After that, component manages state
        metricUrnList: this.get('metricUrnListRoute'),
        metricUrn: this.get('metricUrnRoute')
      });
    }
  },

  /**
   * Helper to map anomalies and start/endpoints over current timestamps
   * @method _mapAnomaliesToTimeSeries
   * @param {Array} filteredAnomalies - The results object from _getAnomalies method
   * @param {Object} timeseries - The object holding timeseries response data
   * @param {Object} series - The object being built to pass to c3js graph
   * @param {String} anomaliesLabel - Name to use as key for anomaly data
   * @param {String} edgesLabel - Name to use as key for anomaly edge points
   * @param {String} color - The color for the anomaly data in the graph
   * @param {Boolean} useValues - Whether to use the current values for anomaly values or not
   * @return {undefined}
   */
  _mapAnomaliesToTimeSeries(
    filteredAnomalies,
    timeseries,
    series,
    anomaliesLabel,
    edgesLabel,
    color,
    useValue) {
    // build set of anomalous values (newer of 2 sets of anomalies)
    if (!_.isEmpty(filteredAnomalies) && timeseries && !_.isEmpty(series.Current)) {
      const valuesCurrent = [];
      // needed because anomalies with startTime before time window are possible
      let currentAnomaly = filteredAnomalies.find(anomaly => {
        return anomaly.startTime <= series.Current.timestamps[0];
      });
      let inAnomalyRange = currentAnomaly ? true : false;
      let anomalyEdgeValues = [];
      let anomalyEdgeTimestamps = [];
      for (let i = 0; i < series.Current.timestamps.length; ++i) {
        const anomalyValue = useValue ? series.Current.values[i] : 1.0;
        const anomalyValueMinusOne = useValue ? series.Current.values[i-1] : 1.0;
        if (!inAnomalyRange) {
          currentAnomaly = filteredAnomalies.find(anomaly => {
            return anomaly.startTime === series.Current.timestamps[i];
          });
          if (currentAnomaly) {
            inAnomalyRange = true;
            valuesCurrent.push(anomalyValue);
            anomalyEdgeValues.push(anomalyValue);
            anomalyEdgeTimestamps.push(series.Current.timestamps[i]);
          } else {
            valuesCurrent.push(null);
          }
        } else if (currentAnomaly.endTime <= series.Current.timestamps[i]) {
          inAnomalyRange = false;
          // we don't want to include the endTime in anomaly range
          currentAnomaly = filteredAnomalies.find(anomaly => {
            return anomaly.startTime === series.Current.timestamps[i];
          });
          if (currentAnomaly) {
            inAnomalyRange = true;
            valuesCurrent.push(anomalyValue);
            anomalyEdgeValues.push(anomalyValue);
            anomalyEdgeTimestamps.push(series.Current.timestamps[i]);
          } else if (i > 0) {
            anomalyEdgeValues.push(anomalyValueMinusOne);
            anomalyEdgeTimestamps.push(series.Current.timestamps[i-1]);
            valuesCurrent.push(null);
          }
        } else {
          valuesCurrent.push(anomalyValue);
        }
      }
      series[anomaliesLabel] = {
        timestamps: series.Current.timestamps,
        values: valuesCurrent,
        type: 'line',
        color
      };
      series[edgesLabel] = {
        timestamps: anomalyEdgeTimestamps,
        values: anomalyEdgeValues,
        type: 'scatter',
        color
      };
      if (!useValue) {
        series[anomaliesLabel].axis = 'y2';
        series[edgesLabel].axis = 'y2';
      }
    }
  },

  /**
   * Helper to reset state if the user is previewing a different metric
   * returns true if the metrics are different
   * @method checkMetricIfCreateAlertPreview
   * @return {boolean}
   */
  _checkMetricIfCreateAlertPreview() {
    let isMetricNew = false;
    const {
      stateOfAnomaliesAndTimeSeries,
      cachedMetric,
      alertYaml
    } = this.getProperties('stateOfAnomaliesAndTimeSeries', 'cachedMetric', 'alertYaml');
    if (stateOfAnomaliesAndTimeSeries === 2 || stateOfAnomaliesAndTimeSeries === 3) {
      if (!this.get('isEditMode')) {
        // is Create Alert preview
        isMetricNew = !(cachedMetric === getValueFromYaml('metric', alertYaml, 'string'));
      }
    }
    return isMetricNew;
  },

  _formattedRule(anomaly) {
    let result;
    if (anomaly.properties && typeof anomaly.properties === 'object') {
      if (anomaly.properties.detectorComponentName) {
        // The format is rule1_name:rule1_type,rule2_name:rule2_type ...
        // For example: wow_10_percent_change:PERCENTAGE_RULE,algorithm:ALGORITHM
        let rules = [];
        anomaly.properties.detectorComponentName.split(',').forEach(x => {
          rules.push(x.split(':')[0]);
        });
        result = rules.sort().join();
      } else if (anomaly.anomalyResultSource) {
        result = (anomaly.anomalyResultSource === 'USER_LABELED_ANOMALY') ? 'User Reported' : '--';
      }
      else {
        result = '--';
      }
    }
    return result;
  },

  _fetchAnomalies() {
    set(this, 'getAnomaliesError', false);
    if (this.get('isPreviewMode')) {
      this.get('sendPreviewError')({
        previewError: false,
        previewErrorMsg: null,
        previewErrorInfo: null
      });
    }

    // If the user is running the detection with a new metric, we should reset the state of time series and anomalies for comparison
    if (this._checkMetricIfCreateAlertPreview()) {
      this.setProperties({
        anomaliesOld: [],
        anomaliesOldSet: false,
        anomaliesNew: [],
        anomaliesNewSet: false
      });
    }

    try {
      // in Edit Alert Preview, we want the original yaml used for comparisons
      const content = (get(this, 'isEditMode') && !(get(this, 'anomaliesOldSet'))) ? get(this, 'originalYaml') : get(this, 'alertYaml');
      return this.get('_getAnomalies').perform(content)
        .then(results => this._setAnomaliesAndTimeSeries(results))
        .then(() => {
          if (get(this, 'metricUrn')) {
            this._fetchTimeseries();
          } else {
            throw new Error('There was no metric or anomaly data returned for the detection');
          }
        })
        .catch(error => {
          if (error.name !== 'TaskCancelation') {
            set(this, 'getAnomaliesError', true);
            if (this.get('isPreviewMode')) {
              this.get('sendPreviewError')({
                previewError: true,
                previewErrorMsg: 'There was an error generating the preview.',
                previewErrorInfo: error
              });
            } else {
              this.get('notifications').error(error, 'Error', toastOptions);
            }
          }
        });
    } catch (error) {
      this.get('notifications').error(error, 'Error', toastOptions);
      set(this, 'getAnomaliesError', true);
      if (this.get('isPreviewMode')) {
        this.get('sendPreviewError')({
          previewError: true,
          previewErrorMsg: 'There was an error generating the preview.',
          previewErrorInfo: error
        });
      }
    }
  },

  _fetchTimeseries() {
    const {
      metricUrn,
      analysisRange,
      selectedBaseline,
      showRules,
      selectedRule,
      uniqueTimeSeries
    } = this.getProperties('metricUrn', 'analysisRange', 'selectedBaseline', 'showRules', 'selectedRule', 'uniqueTimeSeries');
    const timeZone = config.timeZone;

    this.setProperties({
      errorTimeseries: null,
      isLoadingTimeSeries: true
    });
    if (showRules) {
      const seriesSet = uniqueTimeSeries.find(series => {
        if (series.detectorName === selectedRule.detectorName && series.metricUrn === metricUrn) {
          return series;
        }
      });
      if (seriesSet) {
        if (selectedBaseline === 'predicted') {
          this.setProperties({
            timeseries: seriesSet.predictedTimeSeries,
            baseline: seriesSet.predictedTimeSeries,
            isLoadingTimeSeries: false,
            displayRange: [Math.min(seriesSet.predictedTimeSeries.timestamp[0] || analysisRange[0], analysisRange[0]), analysisRange[1]]
          });
        } else {
          const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=${selectedBaseline}&timezone=${timeZone}`;
          fetch(urlBaseline)
            .then(checkStatus)
            .then(res => {
              this.setProperties({
                timeseries: seriesSet.predictedTimeSeries,
                baseline: res,
                isLoadingTimeSeries: false,
                displayRange: [Math.min(seriesSet.predictedTimeSeries.timestamp[0] || analysisRange[0], analysisRange[0]), analysisRange[1]]
              });
            });
        }
      }
    } else {
      const urlCurrent = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=current&timezone=${timeZone}`;
      fetch(urlCurrent)
        .then(checkStatus)
        .then(res => {
          this.setProperties({
            timeseries: res,
            isLoadingTimeSeries: false
          });
        });
      const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=${selectedBaseline}&timezone=${timeZone}`;
      fetch(urlBaseline)
        .then(checkStatus)
        .then(res => set(this, 'baseline', res));
    }
    set(this, 'errorBaseline', null);
  },

  /**
   * Helper function to implement logic for filtering a set of anomalies
   * returns set of anomalies to display in graph based on selections
   * @method _filterAnomalies
   * @param anomalies - an array of anomaly objects
   * @param metricUrn - the metricUrn currently selected for time series chart
   * @param showRules - whether we are showing detection rules and bounds in time series chart
   * @param selectedRule - which detection rule selected for time series chart
   * @return {Array}
   */
  _filterAnomalies(anomalies, metricUrn, showRules, selectedRule) {
    let filteredAnomalies = [];
    if (!_.isEmpty(anomalies)) {
      filteredAnomalies = anomalies.filter(anomaly => {
        if (anomaly.metricUrn === metricUrn) {
          if(showRules && anomaly.properties && typeof anomaly.properties === 'object' && selectedRule && typeof selectedRule === 'object') {
            return ((anomaly.properties.detectorComponentName || '').includes(selectedRule.detectorName));
          } else if (!showRules) {
            // This is necessary until we surface rule selector in Alert Overview
            return true;
          }
        }
        return false;
      });
    }
    return filteredAnomalies;
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(TABLE_DATE_FORMAT)}`;
  },

  _formattedModifiedBy(feedback) {
    let result;
    if (feedback && typeof feedback === 'object') {
      if (feedback.updatedBy && feedback.updatedBy !== 'no-auth-user') {
        result = feedback.updatedBy.split('@')[0];
      } else {
        result = '--';
      }
    }
    return result;
  },

  /**
   * Set retrieved anomalies/timeSeries based on current state
   * @method _setAnomaliesAndTimeSeries
   * @param {Object} results - The results object from _getAnomalies method
   * @return {undefined}
   */
  _setAnomaliesAndTimeSeries(results) {
    const state = get(this, 'stateOfAnomaliesAndTimeSeries');
    switch (state) {
      case 1:
        this.setProperties({
          anomaliesCurrent: results.anomalies,
          anomaliesCurrentSet: true,
          uniqueTimeSeries: results.uniqueTimeSeries
        });
        break;
      case 2:
        this.setProperties({
          anomaliesCurrent: results.anomalies,
          anomaliesCurrentSet: true,
          uniqueTimeSeries: results.uniqueTimeSeries
        });
        break;
      case 3:
        set(this, 'anomaliesOld', this.get('anomaliesCurrent'));
        set(this, 'anomaliesOldSet', true);
        this.setProperties({
          anomaliesCurrent: results.anomalies,
          anomaliesCurrentSet: true,
          uniqueTimeSeries: results.uniqueTimeSeries
        });
        break;
      case 4:
        this.setProperties({
          anomaliesOld: results.anomalies,
          anomaliesOldSet: true,
          anomaliesCurrent: [],
          anomaliesCurrentSet: false
        });
        this._fetchAnomalies();
        break;
      // don't set props if there was an error with _getAnomalies
      default:
        break;
    }
  },

  /**
   * Send a POST request to the report anomaly API (2-step process)
   * http://go/te-ss-alert-flow-api
   * @method reportAnomaly
   * @param {String} id - The alert id
   * @param {Object} data - The input values from 'report new anomaly' modal
   * @return {Promise}
   */
  _reportAnomaly(id, metricUrn, data) {
    const reportUrl = `/detection/report-anomaly/${id}?metricUrn=${metricUrn}`;
    const requiredProps = ['startTime', 'endTime', 'feedbackType'];
    let missingData = false;
    requiredProps.forEach(prop => {
      if (!data[prop]) {
        missingData = true;
      }
    });
    let queryStringUrl = reportUrl;

    if (missingData) {
      return Promise.reject(new Error('missing data'));
    } else {
      Object.entries(data).forEach(([key, value]) => {
        queryStringUrl += `&${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
      });
      // Step 1: Report the anomaly
      return fetch(queryStringUrl, postProps('')).then((res) => checkStatus(res, 'post'));
    }
  },

  /**
   * Modal opener for "report missing anomaly".
   * @method _triggerOpenReportModal
   * @return {undefined}
   */
  _triggerOpenReportModal() {
    this.setProperties({
      isReportSuccess: false,
      isReportFailure: false,
      openReportModal: true
    });
    // We need the C3/D3 graph to render after its containing parent elements are rendered
    // in order to avoid strange overflow effects.
    later(() => {
      this.set('renderModalContent', true);
    });
  },

  actions: {
    /**
     * Handle missing anomaly modal cancel
     */
    onCancel() {
      this.setProperties({
        isReportSuccess: false,
        isReportFailure: false,
        openReportModal: false,
        renderModalContent: false
      });
    },

    /**
     * Open modal for missing anomalies
     */
    onClickReportAnomaly() {
      this._triggerOpenReportModal();
    },

    /**
     * Received bubbled-up action from modal
     * @param {Object} all input field values
     */
    onInputMissingAnomaly(inputObj) {
      this.set('missingAnomalyProps', inputObj);
    },

    /**
     * Handle submission of missing anomaly form from alert-report-modal
     */
    onSave() {
      const { alertId, missingAnomalyProps, metricUrn } = this.getProperties('alertId', 'missingAnomalyProps', 'metricUrn');
      this._reportAnomaly(alertId, metricUrn, missingAnomalyProps)
        .then(() => {
          const rangeFormat = 'YYYY-MM-DD HH:mm';
          const startStr = moment(missingAnomalyProps.startTime).format(rangeFormat);
          const endStr = moment(missingAnomalyProps.endTime).format(rangeFormat);
          this.setProperties({
            isReportSuccess: true,
            isReportFailure: false,
            openReportModal: false,
            reportedRange: `${startStr} - ${endStr}`
          });
        })
        // If failure, leave modal open and report
        .catch(() => {
          this.setProperties({
            missingAnomalyProps: {},
            isReportFailure: true,
            isReportSuccess: false
          });
        });
    },

    onSelectRule(selected) {
      set(this, 'selectedRule', selected);
      this._fetchTimeseries();
    },

    onSelectDimension(selected) {
      const metricUrnList = get(this, 'metricUrnList');
      const newMetricUrn = metricUrnList.find(urn => {
        const dimensionUrn = toMetricLabel(extractTail(decodeURIComponent(urn)));
        if (dimensionUrn === selected) {
          return urn;
          // if there is no tail, this will be called 'All Dimensions' in the UI
        } else if (dimensionUrn === '' && selected === 'All Dimensions') {
          return urn;
        }
      });
      let dimension = toMetricLabel(extractTail(decodeURIComponent(newMetricUrn)));
      dimension = dimension ? dimension : 'All Dimensions';
      this.setProperties({
        metricUrn: newMetricUrn,
        selectedDimension: dimension
      });
      this._fetchTimeseries();
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(start, end) {
      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();

      //Update the time range option selected
      set(this, 'analysisRange', [startDate, endDate]);
      set(this, 'displayRange', [startDate, endDate]);
      // With a new date range, we should reset the state of time series and anomalies for comparison
      if (get(this, 'isPreviewMode')) {
        this.setProperties({
          anomaliesOld: [],
          anomaliesOldSet: false,
          anomaliesCurrent: [],
          anomaliesCurrentSet: false
        });
      }
      // This makes sure we don't fetch if the preview is collapsed
      if(get(this, 'showDetails') && get(this, 'dataIsCurrent')){
        this._fetchAnomalies();
      }
    },

    /**
    * triggered by preview button
    */
    getPreview() {
      this.setProperties({
        showDetails: true,
        dataIsCurrent: true
      });
      this._fetchAnomalies();
    },

    /**
     * Handle display of selected baseline options
     * @param {Object} clicked - the baseline selection
     */
    onBaselineOptionClick(clicked) {
      const baselineOptions = get(this, 'baselineOptions');
      const isValidSelection = !clicked.isActive;
      let newOptions = baselineOptions.map((val) => {
        return { name: val.name, isActive: false };
      });

      // Set active option
      newOptions.find((val) => val.name === clicked.name).isActive = true;
      this.set('baselineOptions', newOptions);

      if(isValidSelection) {
        set(this, 'selectedBaseline', clicked.name);
        this._fetchTimeseries();
      }
    }
  }
});
