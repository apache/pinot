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
import { checkStatus, humanizeFloat, postProps, stripNonFiniteValues } from 'thirdeye-frontend/utils/utils';
import { toastOptions } from 'thirdeye-frontend/utils/constants';
import { colorMapping, makeTime, toMetricLabel, extractTail } from 'thirdeye-frontend/utils/rca-utils';
import { getYamlPreviewAnomalies,
  getAnomaliesByAlertId,
  getFormattedDuration,
  anomalyResponseMapNew,
  anomalyResponseObj,
  anomalyResponseObjNew,
  updateAnomalyFeedback,
  verifyAnomalyFeedback  } from 'thirdeye-frontend/utils/anomaly';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import moment from 'moment';
import _ from 'lodash';

const TABLE_DATE_FORMAT = 'MMM DD, hh:mm A'; // format for anomaly table
const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1m'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['1d', '1w', '1m', '3m'];
const ANOMALY_LEGEND_THRESHOLD = 20; // If number of anomalies is larger than this threshold, don't show the legend

export default Component.extend({
  anomaliesApiService: service('services/api/anomalies'),
  notifications: service('toast'),
  anomalyMapping: {},
  timeseries: null,
  isLoading: false,
  analysisRange: [moment().subtract(1, 'day').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
  isPendingData: false,
  colorMapping: colorMapping,
  zoom: {
    enabled: true,
    rescale: true
  },
  point: {
    show: false
  },
  errorTimeseries: null,
  metricUrn: null,
  metricUrnList: [],
  errorBaseline: null,
  compareMode: 'wo1w',
  baseline: null,
  errorAnomalies: null,
  showDetails: false,
  componentId: 'timeseries-chart',
  anomalies: null,
  sortColumnStartUp: true,
  sortColumnChangeUp: false,
  sortColumnFeedbackUp: false,
  selectedSortMode: 'start:down',
  selectedBaseline: null,
  pageSize: 10,
  currentPage: 1,
  isPreviewMode: false,
  alertId: null,
  alertData: null,
  feedbackOptions: ['Not reviewed yet', 'Yes - unexpected', 'Expected temporary change', 'Expected permanent change', 'No change observed'],
  labelMap: anomalyResponseMapNew,
  labelResponse: {},
  selectedDimension: null,
  isReportSuccess: false,
  isReportFailure: false,
  openReportModal: false,
  missingAnomalyProps: {},
  uniqueTimeSeries: null,
  selectedRule: null,
  isLoadingTimeSeries: false,



  /**
   * This needs to be a computed variable until there is an endpoint for showing predicted with any metricurn
   * @type {Array}
   */
  baselineOptions: computed(
    'isPreviewMode',
    function() {
      let options;
      if (get(this, 'isPreviewMode')) {
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
   * Separate time range for anomalies in preview mode
   * @type {Array}
   */
  anomaliesRange: computed(
    'analysisRange',
    function() {
      const analysisRange = get(this, 'analysisRange');
      let range = [];
      range.push(analysisRange[0]);
      // set end to now if the end time is in the future
      const end = Math.min(moment().valueOf(), analysisRange[1]);
      range.push(end);
      return range;
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
        return [...new Set(uniqueTimeSeries.map(series => {
          const detectorName = series.detectorName;
          const nameOnly = detectorName.split(':')[0];
          return {
            detectorName,
            name: nameOnly
          };
        }))];
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
    'isLoading',
    function() {
      return (get(this, 'isPreviewMode') && get(this, 'isLoading'));
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
        options.push(toMetricLabel(extractTail(decodeURIComponent(urn))));
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
   * Table pagination: number of pages to display
   * @type {Number}
   */
  paginationSize: computed(
    'pagesNum',
    'pageSize',
    function() {
      const { pagesNum, pageSize } = this.getProperties('pagesNum', 'pageSize');
      return Math.min(pagesNum, pageSize/2);
    }
  ),

  /**
   * Table pagination: total Number of pages to display
   * @type {Number}
   */
  pagesNum: computed(
    'tableAnomalies',
    'pageSize',
    function() {
      const { tableAnomalies, pageSize } = this.getProperties('tableAnomalies', 'pageSize');
      const anomalyCount = tableAnomalies.length || 0;
      return Math.ceil(anomalyCount/pageSize);
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

  /**
   * Table pagination: creates the page Array for view
   * @type {Array}
   */
  viewPages: computed(
    'pages',
    'currentPage',
    'paginationSize',
    'pagesNum',
    function() {
      const {
        currentPage,
        pagesNum: max,
        paginationSize: size
      } = this.getProperties('currentPage', 'pagesNum', 'paginationSize');
      const step = Math.floor(size / 2);

      if (max === 1) { return; }

      const startingNumber = ((max - currentPage) < step)
        ? Math.max(max - size + 1, 1)
        : Math.max(currentPage - step, 1);

      return [...new Array(size)].map((page, index) => startingNumber + index);
    }
  ),

  /**
   * Table pagination: pre-filtered and sorted anomalies with pagination
   * @type {Array}
   */
  paginatedFilteredAnomalies: computed(
    'tableAnomalies',
    'pageSize',
    'currentPage',
    'selectedSortMode',
    function() {
      let anomalies = this.get('tableAnomalies');
      const { pageSize, currentPage, selectedSortMode } = getProperties(this, 'pageSize', 'currentPage', 'selectedSortMode');

      if (selectedSortMode) {
        let [ sortKey, sortDir ] = selectedSortMode.split(':');

        if (sortDir === 'up') {
          anomalies = anomalies.sortBy(sortKey);
        } else {
          anomalies = anomalies.sortBy(sortKey).reverse();
        }
      }

      return anomalies.slice((currentPage - 1) * pageSize, currentPage * pageSize);
    }
  ),

  disablePreviewButton: computed(
    'alertYaml',
    'isLoading',
    function() {
      return (get(this, 'alertYaml') === null || get(this, 'isLoading') === true);
    }
  ),

  axis: computed(
    'analysisRange',
    function () {
      const analysisRange = get(this, 'analysisRange');

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
          min: analysisRange[0],
          max: analysisRange[1],
          tick: {
            fit: false,
            format: (d) => {
              const t = makeTime(d);
              if (t.valueOf() === t.clone().startOf('day').valueOf()) {
                return t.format('MMM D (ddd)');
              }
              return t.format('h:mm a');
            }
          }
        }
      };
    }
  ),

  currentAnomalies: computed(
    'anomalies',
    'metricUrn',
    'selectedRule',
    'selectedDimension',
    function() {
      let currentAnomalies = [];
      const {
        metricUrn, anomalies, selectedRule
      } = getProperties(this, 'metricUrn', 'anomalies', 'selectedRule');
      if (!_.isEmpty(anomalies)) {

        currentAnomalies = anomalies.filter(anomaly => {
          if (anomaly.metricUrn === metricUrn) {
            if(get(this, 'isPreviewMode') && anomaly.properties && typeof anomaly.properties === 'object') {
              return (anomaly.properties.detectorComponentName.includes(selectedRule));
            } else if (!get(this, 'isPreviewMode')) {
              // This is necessary until we surface rule selector in Alert Overview
              return true;
            }
          }
          return false;
        });
      }
      return currentAnomalies;
    }
  ),

  legend: computed(
    'numCurrentAnomalies',
    function() {
      if (get(this, 'numCurrentAnomalies') > ANOMALY_LEGEND_THRESHOLD) {
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

  numCurrentAnomalies: computed(
    'currentAnomalies.@each',
    function() {
      const currentAnomalies = get(this, 'currentAnomalies');
      return currentAnomalies.length;
    }
  ),

  series: computed(
    'currentAnomalies.@each',
    'timeseries',
    'baseline',
    'analysisRange',
    'selectedRule',
    'metricUrn',
    function () {
      const {
        currentAnomalies, timeseries, baseline
      } = getProperties(this, 'currentAnomalies', 'timeseries', 'baseline');

      const series = {};
      if (!_.isEmpty(currentAnomalies)) {

        currentAnomalies.forEach(anomaly => {
          const key = this._formatAnomaly(anomaly);
          series[key] = {
            timestamps: [anomaly.startTime, anomaly.endTime],
            values: [1, 1],
            type: 'line',
            color: 'teal',
            axis: 'y2'
          };
          series[key + '-region'] = Object.assign({}, series[key], {
            type: 'region',
            color: 'orange'
          });
        });
      }

      // The current time series has a different naming convention in Preview
      if (get(this, 'isPreviewMode')) {
        if (timeseries && !_.isEmpty(timeseries.current)) {
          series['current'] = {
            timestamps: timeseries.timestamp,
            values: stripNonFiniteValues(timeseries.current),
            type: 'line',
            color: 'grey'
          };
        }
      } else {
        if (timeseries && !_.isEmpty(timeseries.value)) {
          series['current'] = {
            timestamps: timeseries.timestamp,
            values: stripNonFiniteValues(timeseries.value),
            type: 'line',
            color: 'grey'
          };
        }
      }

      if (baseline && !_.isEmpty(baseline.value)) {
        series['baseline'] = {
          timestamps: baseline.timestamp,
          values: stripNonFiniteValues(baseline.value),
          type: 'line',
          color: 'blue'
        };
      }

      if (baseline && !_.isEmpty(baseline.upper_bound)) {
        series['upperBound'] = {
          timestamps: baseline.timestamp,
          values: stripNonFiniteValues(baseline.upper_bound),
          type: 'line',
          color: 'confidence-bounds-blue'
        };
      }

      if (baseline && !_.isEmpty(baseline.lower_bound)) {
        series['lowerBound'] = {
          timestamps: baseline.timestamp,
          values: stripNonFiniteValues(baseline.lower_bound),
          type: 'line',
          color: 'confidence-bounds-blue'
        };
      }
      return series;
    }
  ),

  /**
   * formats anomalies for table
   * @method tableAnomalies
   * @return {Array}
   */
  tableAnomalies: computed(
    'anomalies',
    'labelResponse',
    function() {
      const anomalies = get(this, 'anomalies');
      const labelResponse = get(this, 'labelResponse');
      let tableData = [];

      if (anomalies) {
        anomalies.forEach(a => {
          const change = (a.avgBaselineVal !== 0 && a.avgBaselineVal !== "Infinity" && a.avgCurrentVal !== "Infinity") ? (a.avgCurrentVal/a.avgBaselineVal - 1.0) * 100.0 : 'N/A';
          let tableRow = {
            anomalyId: a.id,
            metricUrn: a.metricUrn,
            start: a.startTime,
            end: a.endTime,
            startDateStr: this._formatAnomaly(a),
            durationStr: getFormattedDuration(a.startTime, a.endTime),
            shownCurrent: a.avgCurrentVal === "Infinity" ? '-' : humanizeFloat(a.avgCurrentVal),
            shownBaseline: a.avgBaselineVal === "Infinity" ? '-' : humanizeFloat(a.avgBaselineVal),
            change: change,
            shownChangeRate: change === 'N/A' ? change : humanizeFloat(change),
            anomalyFeedback: a.feedback ? a.feedback.feedbackType : a.statusClassification,
            dimensionList: Object.keys(a.dimensions),
            dimensions: a.dimensions,
            showResponseSaved: (labelResponse.anomalyId === a.id) ? labelResponse.showResponseSaved : false,
            showResponseFailed: (labelResponse.anomalyId === a.id) ? labelResponse.showResponseFailed: false
          };
          tableData.push(tableRow);
        });
      }
      return tableData;
    }
  ),

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'anomalyMapping',
    function() {
      const {
        anomalyMapping,
        isPreviewMode
      } = this.getProperties('anomalyMapping', 'isPreviewMode');
      if (!anomalyMapping) {
        return {};
      }
      let respondedAnomaliesCount = 0;
      let truePositives = 0;
      let falsePositives = 0;
      let falseNegatives = 0;
      let numberOfAnomalies = 0;
      Object.keys(anomalyMapping).forEach(function (key) {
        anomalyMapping[key].forEach(function (attr) {
          numberOfAnomalies++;
          if(attr.anomaly && attr.anomaly.statusClassification) {
            const classification = attr.anomaly.statusClassification;
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
      });

      const totalAnomaliesCount = numberOfAnomalies;
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      let statsArray = [];
      if(!isPreviewMode) {
        const responseRate = respondedAnomaliesCount / totalAnomaliesCount;
        const precision = truePositives / (truePositives + falsePositives);
        const recall = truePositives / (truePositives + falseNegatives);
        const responseRateDescription = '% of anomalies that are reviewed';
        const precisionDescription = '% of all anomalies detected by the system that are true';
        const recallDescription = '% of all anomalies detected by the system';
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit'],
          ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
          ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
          ['Recall', recallDescription, floatToPercent(recall), 'percent']
        ];
      } else {
        statsArray = [
          ['Anomalies', totalAlertsDescription, totalAnomaliesCount, 'digit']
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
    'analysisRange', 'startDate', 'endDate', 'duration',
    function() {
      const analysisRange = get(this, 'analysisRange');
      const startDate = Number(analysisRange[0]);
      const endDate = Number(analysisRange[1]);
      const duration = get(this, 'duration') || DEFAULT_ACTIVE_DURATION;
      const predefinedRanges = {
        'Today': [moment().startOf('day'), moment().startOf('day').add(1, 'days')],
        'Last 24 hours': [moment().subtract(1, 'day'), moment()],
        'Yesterday': [moment().subtract(1, 'day').startOf('day'), moment().startOf('day')],
        'Last Week': [moment().subtract(1, 'week').startOf('day'), moment().startOf('day')]
      };

      return {
        uiDateFormat: UI_DATE_FORMAT,
        activeRangeStart: moment(startDate).format(DISPLAY_DATE_FORMAT),
        activeRangeEnd: moment(endDate).format(DISPLAY_DATE_FORMAT),
        timeRangeOptions: setUpTimeRangeOptions(TIME_RANGE_OPTIONS, duration),
        timePickerIncrement: TIME_PICKER_INCREMENT,
        predefinedRanges
      };
    }
  ),

  _getAnomalyMapping: task (function * (alertYaml) {//TODO: need to add to anomaly util - LH
    let anomalyMapping = {};
    const {
      analysisRange,
      anomaliesRange,
      notifications,
      isPreviewMode,
      alertId
    } = this.getProperties('analysisRange', 'anomaliesRange', 'notifications', 'isPreviewMode', 'alertId');
    //detection alert fetch
    const start = analysisRange[0];
    const end = analysisRange[1];
    const startAnomalies = anomaliesRange[0];
    const endAnomalies = anomaliesRange[1];
    let anomalies;
    let uniqueTimeSeries;
    let applicationAnomalies;
    let metricUrnList;
    try {
      if(isPreviewMode){
        applicationAnomalies = yield getYamlPreviewAnomalies(alertYaml, startAnomalies, endAnomalies, alertId);
        if (applicationAnomalies && applicationAnomalies.diagnostics && applicationAnomalies.diagnostics['0']) {
          metricUrnList = Object.keys(applicationAnomalies.diagnostics['0']);
          set(this, 'metricUrnList', metricUrnList);
          set(this, 'selectedDimension', toMetricLabel(extractTail(decodeURIComponent(metricUrnList[0]))));
          if (applicationAnomalies.predictions && Array.isArray(applicationAnomalies.predictions) && (typeof applicationAnomalies.predictions[0] === 'object')){
            const detectorName = applicationAnomalies.predictions[0].detectorName;
            const selectedRule = {
              detectorName,
              name: detectorName.split(':')[0]
            };
            set(this, 'selectedRule', selectedRule);
          }
          set(this, 'metricUrn', metricUrnList[0]);
        }
        anomalies = applicationAnomalies.anomalies;
        uniqueTimeSeries = applicationAnomalies.predictions;
      } else {
        applicationAnomalies = yield getAnomaliesByAlertId(alertId, start, end);
        const metricUrnObj = {};
        if (applicationAnomalies) {
          applicationAnomalies.forEach(anomaly => {
            metricUrnObj[anomaly.metricUrn] = 1;
          });
          metricUrnList = Object.keys(metricUrnObj);
          if (metricUrnList.length > 0) {
            set(this, 'metricUrnList', metricUrnList);
            set(this, 'selectedDimension', toMetricLabel(extractTail(decodeURIComponent(metricUrnList[0]))));
            set(this, 'metricUrn', metricUrnList[0]);
          }
        }
        anomalies = applicationAnomalies;
      }

      if (anomalies && anomalies.length > 0) {
        const humanizedObject = {
          queryDuration: '1m',
          queryStart: start,
          queryEnd: end
        };

        anomalies.forEach(anomaly => {
          const metricName = anomaly.metric;
          //Grouping the anomalies of the same metric name
          if (!anomalyMapping[metricName]) {
            anomalyMapping[metricName] = [];
          }

          // Group anomalies by metricName and function name (alertName) and wrap it into the Humanized cache. Each `anomaly` is the raw data from ember data cache.
          anomalyMapping[metricName].push(this.get('anomaliesApiService').getHumanizedEntity(anomaly, humanizedObject));
        });
      }
    } catch (error) {
      if (error.body) {
        notifications.error(error.body.message, 'Error', toastOptions);
      }
    }

    return {
      anomalyMapping,
      anomalies,
      uniqueTimeSeries
    };
  }).drop(),

  init() {
    this._super(...arguments);
    const isPreviewMode = get(this, 'isPreviewMode');
    if (!isPreviewMode) {
      this.setProperties({
        analysisRange: [moment().add(1, 'day').subtract(1, 'month').startOf('day').valueOf(), moment().add(1, 'day').startOf('day').valueOf()],
        duration: '1m',
        selectedDimension: 'Choose a dimension',
        selectedBaseline: 'wo1w'
      });
      this._fetchAnomalies();
    } else {
      this.setProperties({
        duration: '1d',
        selectedBaseline: 'predicted'
      });
    }
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(TABLE_DATE_FORMAT)}`;
  },

  _filterAnomalies(rows) {
    return rows.filter(row => (row.startTime && row.endTime && !row.child));
  },

  _fetchTimeseries() {
    const {
      metricUrn,
      analysisRange,
      selectedBaseline,
      isPreviewMode,
      selectedRule,
      uniqueTimeSeries
    } = this.getProperties('metricUrn', 'analysisRange', 'selectedBaseline', 'isPreviewMode', 'selectedRule', 'uniqueTimeSeries');
    const timeZone = 'America/Los_Angeles';

    this.setProperties({
      errorTimeseries: null,
      isLoadingTimeSeries: true
    });

    if (isPreviewMode) {
      const seriesSet = uniqueTimeSeries.find(series => {
        if (series.detectorName === selectedRule.detectorName && series.metricUrn === metricUrn) {
          return series;
        }
      });
      if (selectedBaseline === 'predicted') {
        this.setProperties({
          timeseries: seriesSet.predictedTimeSeries,
          baseline: seriesSet.predictedTimeSeries,
          isLoadingTimeSeries: false
        });
      } else {
        const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=${selectedBaseline}&timezone=${timeZone}`;
        fetch(urlBaseline)
          .then(checkStatus)
          .then(res => {
            this.setProperties({
              timeseries: seriesSet.predictedTimeSeries,
              baseline: res,
              isLoadingTimeSeries: false
            });
          });
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

  _fetchAnomalies() {
    this.setProperties({
      errorAnomalies: null,
      isLoading: true
    });

    try {
      const content = get(this, 'alertYaml');
      this.get('_getAnomalyMapping').perform(content)
        .then(results => {
          this.setProperties({
            anomalyMapping: results.anomalyMapping,
            anomalies: results.anomalies,
            uniqueTimeSeries: results.uniqueTimeSeries,
            isLoading: false
          });
          if (get(this, 'metricUrn')) {
            this._fetchTimeseries();
          } else {
            throw new Error('Unable to get MetricUrn from response');
          }
        });
    } catch (error) {
      set(this, 'isLoading', false);
      throw new Error(`Unable to retrieve anomaly data. ${error}`);
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
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} anomalyRecord - the anomaly being responded to
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     */
    onChangeAnomalyFeedback: async function(anomalyRecord, selectedResponse) {
      const anomalies = get(this, 'anomalies');
      // Reset status icon
      set(this, 'renderStatusIcon', false);
      const responseObj = anomalyResponseObj.find(res => res.name === selectedResponse);
      // get the response object from anomalyResponseObjNew
      const newFeedbackValue = anomalyResponseObjNew.find(res => res.name === selectedResponse).value;
      try {
        // Save anomaly feedback
        await updateAnomalyFeedback(anomalyRecord.anomalyId, responseObj.value);
        // We make a call to ensure our new response got saved
        const anomaly = await verifyAnomalyFeedback(anomalyRecord.anomalyId);

        if (anomaly.feedback && responseObj.value === anomaly.feedback.feedbackType) {
          this.set('labelResponse', {
            anomalyId: anomalyRecord.anomalyId,
            showResponseSaved: true,
            showResponseFailed: false
          });

          // replace anomaly feedback with selectedFeedback
          let i = 0;
          let found = false;
          while (i < anomalies.length && !found) {
            if (anomalies[i].id === anomalyRecord.anomalyId) {
              if (anomalies[i].feedback) {
                anomalies[i].feedback.feedbackType = newFeedbackValue;
              } else {
                anomalies[i].feedback = {
                  feedbackType: newFeedbackValue
                };
              }
              found = true;
            }
            i++;
          }
          set(this, 'anomalies', anomalies);
        } else {
          throw 'Response not saved';
        }
      } catch (err) {
        this.set('labelResponse', {
          anomalyId: anomalyRecord.anomalyId,
          showResponseSaved: false,
          showResponseFailed: true
        });
      }
      // Force status icon to refresh
      set(this, 'renderStatusIcon', true);
    },

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
        if (toMetricLabel(extractTail(decodeURIComponent(urn))) === selected) {
          return urn;
        }
      });
      this.setProperties({
        metricUrn: newMetricUrn,
        selectedDimension: toMetricLabel(extractTail(decodeURIComponent(newMetricUrn)))
      });
      this._fetchTimeseries();
    },

    /**
      * Action handler for page clicks
      * @param {Number|String} page
      */
    onPaginationClick(page) {
      let newPage = page;
      let currentPage = this.get('currentPage');

      switch (page) {
        case 'previous':
          if (currentPage > 1) {
            newPage = --currentPage;
          } else {
            newPage = currentPage;
          }
          break;
        case 'next':
          if (currentPage < this.get('pagesNum')) {
            newPage = ++currentPage;
          } else {
            newPage = currentPage;
          }
          break;
      }
      this.set('currentPage', newPage);
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(timeRangeOptions) {
      const {
        start,
        end,
        value: duration
      } = timeRangeOptions;

      const startDate = moment(start).valueOf();
      const endDate = moment(end).valueOf();
      //Update the time range option selected
      set(this, 'analysisRange', [startDate, endDate]);
      set(this, 'duration', duration);
      this._fetchAnomalies();
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
    },

    /**
     * Handle sorting for each sortable table column
     * @param {String} sortKey  - stringified start date
     */
    toggleSortDirection(sortKey) {
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';

      this.toggleProperty(propName);
      if (this.get(propName)) {
        this.set('selectedSortMode', sortKey + ':up');
      } else {
        this.set('selectedSortMode', sortKey + ':down');
      }

      //On sort, set table to first pagination page
      this.set('currentPage', 1);
    }
  }
});
