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
import { computed, observer, set, get, getProperties } from '@ember/object';
import { later } from '@ember/runloop';
import { checkStatus, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import { colorMapping, toColor, makeTime } from 'thirdeye-frontend/utils/rca-utils';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { setUpTimeRangeOptions } from 'thirdeye-frontend/utils/manage-alert-utils';
import moment from 'moment';
import _ from 'lodash';
import d3 from 'd3';

const TABLE_DATE_FORMAT = 'MMM DD, hh:mm A'; // format for anomaly table
const TIME_PICKER_INCREMENT = 5; // tells date picker hours field how granularly to display time
const DEFAULT_ACTIVE_DURATION = '1m'; // setting this date range selection as default (Last 24 Hours)
const UI_DATE_FORMAT = 'MMM D, YYYY hh:mm a'; // format for date picker to use (usually varies by route or metric)
const DISPLAY_DATE_FORMAT = 'YYYY-MM-DD HH:mm'; // format used consistently across app to display custom date range
const TIME_RANGE_OPTIONS = ['1m', '3m'];

export default Component.extend({
  anomaliesApiService: service('services/api/anomalies'),
  notifications: service('toast'),
  anomalyMapping: {},
  timeseries: null,
  isLoading: false,
  analysisRange: [moment().subtract(1, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],
  isPendingData: false,
  colorMapping: colorMapping,
  zoom: {
    enabled: true,
    rescale: true
  },

  legend: {
    show: true,
    position: 'right'
  },
  errorTimeseries: null,
  metricUrn: null,
  errorBaseline: null,
  compareMode: 'wo1w',
  baseline: null,
  errorAnomalies: null,
  showPreview: false,
  componentId: 'timeseries-chart',
  anomalies: null,
  baselineOptions: [
    { name: 'wo1w', isActive: true},
    { name: 'wo2w', isActive: false},
    { name: 'wo3w', isActive: false},
    { name: 'wo4w', isActive: false},
    { name: 'mean4w', isActive: false},
    { name: 'median4w', isActive: false},
    { name: 'min4w', isActive: false},
    { name: 'max4w', isActive: false},
    { name: 'none', isActive: false}
  ],
  sortColumnStartUp: false,
  sortColumnChangeUp: false,
  sortColumnNumberUp: true,
  sortColumnResolutionUp: false,
  selectedSortMode: '',
  selectedBaseline: 'wo1w',
  pageSize: 10,
  currentPage: 1,


  // alertYamlChanged: observer('alertYaml', 'analysisRange', 'disableYamlSave', async function() {
  //   set(this, 'isPendingData', true);
  //   // deal with the change
  //   const alertYaml = get(this, 'alertYaml');
  //   if(alertYaml) {
  //     try {
  //       const anomalyMapping = await this.get('_getAnomalyMapping').perform(alertYaml);
  //       set(this, 'isPendingData', false);
  //       set(this, 'anomalyMapping', anomalyMapping);
  //       debugger;
  //     } catch (error) {
  //       throw new Error(`Unable to retrieve anomaly data. ${error}`);
  //     }
  //   }
  // }),

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
      const analysisRange = getProperties(this, 'analysisRange');

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

  series: computed(
    'anomalies',
    'timeseries',
    'baseline',
    'analysisRange',
    function () {
      const {
        metricUrn, anomalies, timeseries, baseline, analysisRange
      } = getProperties(this, 'metricUrn', 'anomalies', 'timeseries',
        'baseline', 'analysisRange');

      const series = {};

      if (!_.isEmpty(anomalies)) {

          anomalies
            .filter(anomaly => anomaly.metricUrn === metricUrn)
            .forEach(anomaly => {
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

      if (timeseries && !_.isEmpty(timeseries.value)) {
        series['current'] = {
          timestamps: timeseries.timestamp,
          values: timeseries.value,
          type: 'line',
          color: toColor(metricUrn)
        };
      }

      if (baseline && !_.isEmpty(baseline.value)) {
        series['baseline'] = {
          timestamps: baseline.timestamp,
          values: baseline.value,
          type: 'line',
          color: 'light-' + toColor(metricUrn)
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
    function() {
      const anomalies = get(this, 'anomalies');
      let tableData = [];
      let i = 1;

      anomalies.forEach(a => {
        let tableRow = {
          number: i,
          start: a,
          startDateStr: this._formatAnomaly(a),
          shownCurrent: humanizeFloat(a.avgCurrentVal),
          shownBaseline: humanizeFloat(a.avgBaselineVal),
          change: ((a.avgCurrentVal/a.avgBaselineVal - 1.0) * 100.0),
          shownChangeRate: humanizeFloat(((a.avgCurrentVal/a.avgBaselineVal - 1.0) * 100.0))
        };
        tableData.push(tableRow);
        i++;
      });
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
      const anomalyMapping = get(this, 'anomalyMapping');
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
          if(attr.anomaly && attr.anomaly.data) {
            const classification = attr.anomaly.data.classification;
            if (classification != 'NONE') {
              respondedAnomaliesCount++;
              if (classification == 'TRUE_POSITIVE') {
                truePositives++;
              } else if (classification == 'FALSE_POSITIVE') {
                falsePositives++;
              } else if (classification == 'FALSE_NEGATIVE') {
                falseNegatives++;
              }
            }
          }
        });
      });

      const totalAnomaliesCount = numberOfAnomalies;
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      let statsArray = [];
      if(respondedAnomaliesCount > 0) {
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
      const startDate = Number(analysisRange[0]) || Number(get(this, 'startDate'));
      const endDate = Number(analysisRange[1]) || Number(get(this, 'endDate'));
      const duration = get(this, 'duration') || DEFAULT_ACTIVE_DURATION;
      const predefinedRanges = {
        'Today': [moment().startOf('day'), moment()],
        'Last 24 hours': [moment().subtract(1, 'day'), moment()],
        'Yesterday': [moment().subtract(1, 'day').startOf('day'), moment().subtract(1, 'days').endOf('day')],
        'Last Week': [moment().subtract(1, 'week'), moment()]
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
    const analysisRange = get(this, 'analysisRange');
    const postProps = {
      method: 'POST',
      body: alertYaml,
      headers: { 'content-type': 'text/plain' }
    };
    const notifications = get(this, 'notifications');

    //detection alert fetch
    const start = analysisRange[0];
    const end = analysisRange[1];
    const alertUrl = `/yaml/preview?start=${start}&end=${end}&tuningStart=0&tuningEnd=0`;
    let anomalies;
    try {
      const alert_result = yield fetch(alertUrl, postProps);
      const alert_status  = get(alert_result, 'status');
      const applicationAnomalies = yield alert_result.json();

      if (alert_status !== 200 && applicationAnomalies.message) {
        notifications.error(applicationAnomalies.message, 'Preview alert failed');
      } else {
        anomalies = applicationAnomalies.anomalies;
        set(this, 'metricUrn', Object.keys(applicationAnomalies.diagnostics['0'])[0]);

        if (anomalies && anomalies.length > 0) {
          const humanizedObject = {
            queryDuration: '1m',
            queryStart: start,
            queryEnd: end
          };
          this.set('applicationAnomalies', anomalies);

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
      }
    } catch (error) {
      notifications.error('Preview alert failed', error);
    }

    return {
      anomalyMapping,
      anomalies
    };
  }).drop(),

  didRender(){
    this._super(...arguments);

    later(() => {
      this._buildSliderButton();
    });
  },

  // Helper function that builds the subchart region buttons
  _buildSliderButton() {
    const componentId = this.get('componentId');
    const resizeButtons = d3.select(`.${componentId}`).selectAll('.resize');

    resizeButtons.append('circle')
      .attr('cx', 0)
      .attr('cy', 30)
      .attr('r', 10)
      .attr('fill', '#0091CA');
    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 0)
      .attr("y1", 27)
      .attr("x2", 0)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", -5)
      .attr("y1", 27)
      .attr("x2", -5)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 5)
      .attr("y1", 27)
      .attr("x2", 5)
      .attr("y2", 33);
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
      selectedBaseline
    } = this.getProperties('metricUrn', 'analysisRange', 'selectedBaseline');
    const granularity = '15_MINUTES';
    const timezone = moment.tz.guess();

    set(this, 'errorTimeseries', null);

    const urlCurrent = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=current&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlCurrent)
      .then(checkStatus)
      .then(res => {
        this.setProperties({
          timeseries: res,
          isLoading: false
        });
      });

    set(this, 'errorBaseline', null);

    const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${analysisRange[0]}&end=${analysisRange[1]}&offset=${selectedBaseline}&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlBaseline)
      .then(checkStatus)
      .then(res => set(this, 'baseline', res));
  },

  _fetchAnomalies() {
    set(this, 'errorAnomalies', null);

    try {
      const content = get(this, 'alertYaml');
      this.get('_getAnomalyMapping').perform(content)
      .then(results => {
        this.setProperties({
          anomalyMapping: results.anomalyMapping,
          anomalies: results.anomalies,
          isLoading: false
        });
        this._fetchTimeseries();
      });
    } catch (error) {
      set(this, 'isLoading', false);
      throw new Error(`Unable to retrieve anomaly data. ${error}`);
    }
  },

  actions: {
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
    },

    /**
    * triggered by preview button
    */
    getPreview() {
      this.setProperties({
        isLoading: true,
        showPreview: true
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
    },

    refreshPreview(){
      set(this, 'disableYamlSave', true);
    }
  }
});
