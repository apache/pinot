/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import Controller from '@ember/controller';
import { computed } from '@ember/object';
import { task, timeout } from 'ember-concurrency';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';

export default Controller.extend({
  /**
   * Be ready to receive trigger for loading new ux with redirect to alert page
   */
  queryParams: ['newUx'],
  newUx: null,

  /**
   * Initialized alert creation page settings
   */
  isValidated: false,
  isMetricSelected: false,
  isFormDisabled: false,
  isMetricDataInvalid: false,
  isCreateAlertSuccess: false,
  isCreateGroupSuccess: false,
  isCreateAlertError: false,
  isSelectMetricError: false,
  isReplayStatusError: false,
  isMetricDataLoading: false,
  isReplayStatusPending: true,
  isReplayStatusSuccess: false,
  isReplayStarted: false,
  isGroupNameDuplicate: false,
  isAlertNameDuplicate: false,
  isFetchingDimensions: false,
  isDimensionFetchDone: false,
  isProcessingForm: false,
  isEmailError: false,
  isDuplicateEmail: false,
  showGraphLegend: false,
  redirectToAlertPage: true,
  metricGranularityOptions: [],
  topDimensions: [],
  originalDimensions: [],
  bsAlertBannerType: 'success',
  graphEmailLinkProps: '',
  replayStatusClass: 'te-form__banner--pending',
  legendText: {
    dotted: {
      text: 'WoW'
    },
    solid: {
      text: 'Observed'
    }
  },

  /**
   * Change this to activate new alert anomaly page redirect
   */
  isNewUx: Ember.computed.reads('model.isNewUx'),

  /**
   * Component property initial settings
   */
  filters: {},
  graphConfig: {},
  selectedFilters: JSON.stringify({}),
  selectedSensitivity: null,
  selectedWeeklyEffect: true,

  /**
   * Object to cover basic ield 'presence' validation
   */
  requiredFields: [
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName'
  ],

  /**
   * Array to define alerts table columns for selected config group
   */
  alertsTableColumns: [
    {
      propertyName: 'id',
      title: 'Id',
      className: 'te-form__table-index'
    },
    {
      propertyName: 'name',
      title: 'Alert Name'
    },
    {
      propertyName: 'metric',
      title: 'Alert Metric'
    },
    {
      propertyName: 'type',
      title: 'Alert Type'
    }
  ],

  /**
   * Options for patterns of interest field. These may eventually load from the backend.
   */
  patternsOfInterest: ['Up and Down', 'Up only', 'Down only'],

  /**
   * Options for sensitivity field, previously 'Robust', 'Medium', 'Sensitive'
   */
  sensitivityOptions: ['Robust (Low)', 'Medium', 'Sensitive (High)'],

  /**
   * Mapping user readable pattern and sensitivity to DB values
   */
  optionMap: {
    pattern: {
      'Up and Down': 'UP,DOWN',
      'Up only': 'UP',
      'Down only': 'DOWN'
    },
    sensitivity: {
      'Robust (Low)': 'LOW',
      'Medium': 'MEDIUM',
      'Sensitive (High)': 'HIGH'
    },
    severity: {
      'Percentage of Change': 'weight',
      'Absolute Value of Change': 'deviation'
    }
  },

  /**
   * Severity display options (power-select) and values
   * @type {Object}
   */
  tuneSeverityOptions: computed(
    'optionMap.severity',
    function() {
      const severityOptions = Object.keys(this.get('optionMap.severity'));
      return severityOptions;
    }
  ),

  /**
   * Conditionally display '%' based on selected severity option
   * @type {String}
   */
  sensitivityUnits: computed('selectedSeverityOption', function() {
    const chosenSeverity = this.get('selectedSeverityOption');
    const isNotPercent = chosenSeverity && chosenSeverity.includes('Absolute');
    return isNotPercent ? '' : '%';
  }),

  /**
   * Builds the new autotune filter from custom tuning options
   * @type {String}
   */
  alertFilterObj: computed(
    'selectedSeverityOption',
    'customPercentChange',
    'customMttdChange',
    'selectedPattern',
    function() {
      const {
        severity: severityMap,
        pattern: patternMap
      } = this.getProperties('optionMap').optionMap;

      const {
        selectedPattern,
        customMttdChange,
        customPercentChange,
        selectedSeverityOption: selectedSeverity
      } = this.getProperties('selectedPattern', 'customMttdChange', 'customPercentChange', 'selectedSeverityOption');

      const requiredProps = ['customMttdChange', 'customPercentChange', 'selectedSeverityOption'];
      const isCustomFilterPossible = requiredProps.every(val => Ember.isPresent(this.get(val)));
      const filterObj = { pattern: patternMap[selectedPattern] };

      if (isCustomFilterPossible) {
        const mttdVal = Number(customMttdChange).toFixed(2);
        const severityThresholdVal = (Number(customPercentChange)/100).toFixed(2);
        // NOTE: finalStr will be used in next iteration
        // const finalStr = `&features=${encodeURIComponent(featureString)}&mttd=${encodeURIComponent(mttdString)}${patternString}`;
        Object.assign(filterObj, {
          features: `window_size_in_hour,${severityMap[selectedSeverity]}`,
          mttd: `window_size_in_hour=${mttdVal};${severityMap[selectedSeverity]}=${severityThresholdVal}`
        });
      }

      return filterObj;
    }
  ),

  /**
   * All selected dimensions to be loaded into graph
   * @returns {Array}
   */
  selectedDimensions: computed(
    'topDimensions',
    'topDimensions.@each.isSelected',
    function() {
      return this.get('topDimensions').filterBy('isSelected');
    }
  ),

  /**
   * Setting default sensitivity if selectedSensitivity is undefined
   * @returns {String}
   */
  sensitivityWithDefault: computed(
    'selectedSensitivity',
    'selectedGranularity',
    function() {
      let {
        selectedSensitivity,
        selectedGranularity
      } =  this.getProperties('selectedSensitivity', 'selectedGranularity');

      if (!selectedSensitivity) {
        const isDailyOrHourly = ['DAYS', 'HOURS'].includes(selectedGranularity);
        selectedSensitivity = isDailyOrHourly ? 'Sensitive (High)' : 'Medium';
      }

      return this.optionMap.sensitivity[selectedSensitivity];
    }
  ),

  weeklyEffectOptions: [true, false],
  /**
   * Application name field options loaded from our model.
   */
  allApplicationNames: Ember.computed.reads('model.allAppNames'),

  /**
   * The list of all existing alert configuration groups.
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allConfigGroups'),

  /**
   * Handler for search by function name - using ember concurrency (task)
   * @method searchMetricsList
   * @param {metric} String - portion of metric name used in typeahead
   * @return {Promise}
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url).then(checkStatus);
  }),

  /**
   * Pseudo-encodes the querystring params to be posted. NOTE: URI encoders cannot be used
   * here because they will encode the 'cron' property value, which causes the request to fail.
   * Previously tried ${encodeURI(key)}=${encodeURI(paramsObj[key])}
   * @method toQueryString
   * @param {Object} paramsObj - the object we are flattening and url-encoding
   * @return {String}
   */
  toQueryString(paramsObj) {
    return Object
      .keys(paramsObj)
      .map(key => `${key}=${paramsObj[key]}`)
      .join('&');
  },

  /**
   * Sets error message to alert create error state and disables form
   * @method setAlertCreateErrorState
   * @returns {undefined}
   */
  setAlertCreateErrorState(error) {
    // TODO: Log alert creation errors with Piwik
    this.setProperties({
      isCreateAlertError: true,
      isFormDisabled: true
    });
  },

  /**
   * Determines if a metric should be filtered out
   * @method isMetricGraphable
   * @param {Object} metric
   * @returns {Boolean}
   */
  isMetricGraphable(metric) {
    return metric
    && metric.subDimensionContributionMap['All'].currentValues
    && metric.subDimensionContributionMap['All'].currentValues.reduce((total, val) => {
      return total + val;
    }, 0);
  },

  /**
   * Fetches an alert function record by Id.
   * Use case: show me the names of all functions monitored by a given alert group.
   * @method fetchFunctionById
   * @param {Number} functionId - Id for the selected alert function
   * @return {Promise}
   */
  fetchFunctionById(functionId) {
    const url = `/onboard/function/${functionId}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  fetchAnomalyByName(functionName) {
    const url = `/data/autocomplete/functionByName?name=${functionName}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * Note: these requests can fail silently and any empty response will fall back on defaults.
   * @method fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {Ember.RSVP.promise}
   */
  fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => checkStatus(res, 'get', true)),
      granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res => checkStatus(res, 'get', true))
    };
    return Ember.RSVP.hash(promiseHash);
  },

  /**
   * Fetches the time series data required to display the anomaly detection graph for the current metric.
   * @method fetchAnomalyGraphData
   * @param {Object} config - key metric properties to graph
   * @return {Promise} Returns time-series data for the metric
   */
  fetchAnomalyGraphData(config) {
    const {
      id,
      dimension,
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity,
      filters
    } = config;
    const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}&filters=${encodeURIComponent(filters)}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Send a POST request to the entity endpoint to create a new record.
   * @method saveThirdEyeEntity
   * @param {Object} alertData - The record being saved (in this case, a new alert config group)
   * @param {String} entityType - The type of entity being saved
   * @return {Promise}
   */
  saveThirdEyeEntity(alertData, entityType) {
    const postProps = {
      method: 'post',
      body: JSON.stringify(alertData),
      headers: { 'content-type': 'Application/Json' }
    };
    const url = '/thirdeye/entity?entityType=' + entityType;
    return fetch(url, postProps).then((res) => checkStatus(res, 'post'));
  },

  /**
   * Send a POST request to the new function create endpoint.
   * @method saveThirdEyeFunction
   * @param {Object} functionData - The new function to save
   * @return {Promise}
   */
  saveThirdEyeFunction(functionData) {
    const postProps = {
      method: 'post',
      headers: { 'content-type': 'Application/Json' }
    };
    const url = '/dashboard/anomaly-function?' + this.toQueryString(functionData);
    return fetch(url, postProps).then(checkStatus);
  },

  /**
   * Send a DELETE request to the function delete endpoint.
   * @method removeThirdEyeFunction
   * @param {Object} functionId - The id of the alert to remove
   * @return {Promise}
   */
  removeThirdEyeFunction(functionId) {
    const postProps = {
      method: 'delete',
      headers: { 'content-type': 'text/plain' }
    };
    const url = '/dashboard/anomaly-function?id=' + functionId;
    return fetch(url, postProps).then(checkStatus);
  },

  /**
   * Loads time-series data into the anomaly-graph component.
   * Note: 'MINUTE' granularity loads 1 week of data. Otherwise, it loads 1 month.
   * @method triggerGraphFromMetric
   * @param {Number} metricId - Id of selected metric to graph
   * @return {undefined}
   */
  triggerGraphFromMetric(metricId) {
    const id = metricId.id;
    const maxDimensionSize = 5;
    const maxTime = this.get('maxTime');
    const selectedDimension = this.get('selectedDimension');
    const filters = this.get('selectedFilters') || '';
    const dimension = selectedDimension || 'All';
    const currentEnd = moment(maxTime).isValid()
      ? moment(maxTime).valueOf()
      : buildDateEod(1, 'day').valueOf();
    const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
    const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(1, 'week');
    const granularity = this.get('selectedGranularity') || this.get('granularities.firstObject') || '';
    const isMinutely = granularity.toLowerCase().includes('minute');
    const graphConfig = {
      id,
      dimension,
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity,
      filters
    };

    // Reduce data volume by narrowing graph window to 2 weeks for minute granularity
    if (isMinutely) {
      graphConfig.currentStart = moment(currentEnd).subtract(2, 'week').valueOf();
    }

    // Update graph, and related fields
    this.setProperties({
      graphConfig: graphConfig,
      selectedGranularity: granularity,
      isFilterSelectDisabled: Ember.isEmpty(filters)
    });

    // Fetch new graph metric data
    this.fetchAnomalyGraphData(graphConfig).then(metricData => {
      if (!this.isMetricGraphable(metricData)) {
        // Metric has no data. not graphing
        this.setProperties({
          isMetricDataInvalid: true,
          isMetricDataLoading: false
        });
      } else {
        // Dimensions are selected. Compile, rank, and send them to the graph.
        if(selectedDimension) {
          this.getTopDimensions(metricData, graphConfig, maxDimensionSize, selectedDimension)
            .then(orderedDimensions => {
              this.setProperties({
                isMetricSelected: true,
                isFetchingDimensions: false,
                isDimensionFetchDone: true,
                isMetricDataLoading: false,
                topDimensions: orderedDimensions
              });
            })
            .catch(() => {
              this.set('isMetricDataLoading', false);
            });
        }
        // Metric has data. now sending new data to graph.
        this.setProperties({
          isMetricSelected: true,
          isMetricDataLoading: false,
          showGraphLegend: Ember.isPresent(selectedDimension),
          selectedMetric: Object.assign(metricData, { color: 'blue' })
        });
      }
    }).catch((error) => {
      // The request failed. No graph to render.
      this.clearAll();
      this.setProperties({
        isMetricDataLoading: false,
        selectMetricErrMsg: error
      });
    });
  },

  /**
   * If a dimension has been selected, the metric data object will contain subdimensions.
   * This method calls for dimension ranking by metric, filters for the selected dimension,
   * and returns a sorted list of graph-ready dimension objects.
   * @method getTopDimensions
   * @param {Object} data - the graphable metric data returned from fetchAnomalyGraphData()
   * @param {Object} config - the graph configuration object
   * @param {Number} maxSize - number of sub-dimensions to display on graph
   * @param {String} selectedDimension - the user-selected dimension to graph
   * @return {undefined}
   */
  getTopDimensions(data, config, maxSize, selectedDimension) {
    const url = `/rootcause/query?framework=relatedDimensions&anomalyStart=${config.currentStart}&anomalyEnd=${config.currentEnd}&baselineStart=${config.baselineStart}&baselineEnd=${config.baselineEnd}&analysisStart=${config.currentStart}&analysisEnd=${config.currentEnd}&urns=thirdeye:metric:${config.id}&filters=${encodeURIComponent(config.filters)}`;
    const colors = ['orange', 'teal', 'purple', 'red', 'green', 'pink'];
    const dimensionObj = data.subDimensionContributionMap || {};
    let dimensionList = [];
    let topDimensions = [];
    let topDimensionLabels = [];
    let filteredDimensions = [];
    let colorIndex = 0;

    return new Ember.RSVP.Promise((resolve) => {
      fetch(url).then(checkStatus)
        .then((scoredDimensions) => {
          // Select scored dimensions belonging the selected one
          filteredDimensions =  _.filter(scoredDimensions, function(dimension) {
            return dimension.label.split('=')[0] === selectedDimension;
          });
          // Prep a sorted list of labels for our dimension's top contributing sub-dimensions
          topDimensions = filteredDimensions.sortBy('score').reverse().slice(0, maxSize);
          topDimensionLabels = [...new Set(topDimensions.map(key => key.label.split('=')[1]))];
          // Build the array of subdimension objects for the selected dimension
          for (let subDimension of topDimensionLabels) {
            if (subDimension && dimensionObj[subDimension]) {
              dimensionList.push({
                name: subDimension,
                color: colors[colorIndex],
                baselineValues: dimensionObj[subDimension].baselineValues,
                currentValues: dimensionObj[subDimension].currentValues,
                isSelected: true
              });
              colorIndex++;
            }
          }
          // Return sorted list of dimension objects
          resolve(dimensionList);
        });
    });
  },

  /**
   * Replay Flow Step 1 - Clones an alert function in preparation for replay.
   * @method callCloneAlert
   * @param {Number} functionId - the newly created function's id
   * @return {Ember.RSVP.Promise}
   */
  callCloneAlert(functionId) {
    const url = `/onboard/function/${functionId}/clone/cloned`;
    return fetch(url, { method: 'post' }).then((res) => checkStatus(res, 'post'));
  },

  /**
   * Generate the URL needed to trigger replay for new alert
   * @method buildReplayUrl
   * @param {Number} functionId - the newly created function's id
   * @return {String}
   */
  buildReplayUrl(functionId) {
    const replayDateFormat = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
    const startTime = buildDateEod(1, 'month').format(replayDateFormat);
    const endTime = buildDateEod(1, 'day').format(replayDateFormat);
    const granularity = this.get('graphConfig.granularity').toLowerCase();
    const speedUp = !(granularity.includes('hour') || granularity.includes('day'));
    const recipients = this.get('selectedConfigGroup.recipients');
    const sensitivity = this.get('sensitivityWithDefault');
    const selectedPattern = this.get('selectedPattern');
    const pattern = this.optionMap.pattern[selectedPattern];

    return `/detection-job/${functionId}/notifyreplaytuning?start=${startTime}` +
      `&end=${endTime}&speedup=${speedUp}&userDefinedPattern=${pattern}&sensitivity=${sensitivity}` +
      `&removeAnomaliesInWindow=true&removeAnomaliesInWindow=true&to=${recipients}`;
  },

  /**
   * Enriches the list of functions by Id, adding the properties we may want to display.
   * We are preparing to display the alerts that belong to the currently selected config group.
   * @method prepareFunctions
   * @param {Object} configGroup - the currently selected alert config group
   * @param {Object} newId - conditional param to help us tag any function that was "just added"
   * @return {Ember.RSVP.Promise} A new list of functions (alerts)
   */
  prepareFunctions(configGroup, newId = 0) {
    const newFunctionList = [];
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    let cnt = 0;

    // Build object for each function(alert) to display in results table
    return new Ember.RSVP.Promise((resolve) => {
      for (var functionId of existingFunctionList) {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push({
            number: cnt + 1,
            id: functionData.id,
            name: functionData.functionName,
            metric: functionData.metric + '::' + functionData.collection,
            type: functionData.type,
            active: functionData.isActive,
            isNewId: functionData.id === newId
          });
          cnt ++;
          if (existingFunctionList.length === cnt) {
            if (newId) {
              newFunctionList.reverse();
            }
            resolve(newFunctionList);
          }
        });
      }
    });
  },

  /**
   * If these two conditions are true, we assume the user wants to edit an existing alert group
   * @method isAlertGroupEditModeActive
   * @return {Boolean}
   */
  isAlertGroupEditModeActive: computed(
    'selectedConfigGroup',
    'newConfigGroupName',
    function() {
      return this.get('selectedConfigGroup') && Ember.isNone(this.get('newConfigGroupName'));
    }
  ),

  /**
   * Determines cases in which the filter field should be disabled
   * @method isFilterSelectDisabled
   * @return {Boolean}
   */
  isFilterSelectDisabled: computed(
    'filters',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || Ember.isEmpty(this.get('filters')));
    }
  ),

  /**
   * Determines cases in which the granularity field should be disabled
   * @method isGranularitySelectDisabled
   * @return {Boolean}
   */
  isGranularitySelectDisabled: computed(
    'granularities',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || Ember.isEmpty(this.get('granularities')));
    }
  ),

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @param {Number} metricId - Id of selected metric to graph
   * @return {Boolean} PreventSubmit
   */
  isSubmitDisabled: computed(
    'selectedMetricOption',
    'selectedPattern',
    'selectedWeeklyEffect',
    'alertFunctionName',
    'selectedAppName',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    'isAlertNameDuplicate',
    'isGroupNameDuplicate',
    'isProcessingForm',
    function() {
      let isDisabled = false;
      const {
        requiredFields,
        isProcessingForm,
        newConfigGroupName,
        isAlertNameDuplicate,
        isGroupNameDuplicate,
        alertGroupNewRecipient,
        selectedConfigGroup: groupRecipients,
      } = this.getProperties(
        'requiredFields',
        'isProcessingForm',
        'newConfigGroupName',
        'isAlertNameDuplicate',
        'isGroupNameDuplicate',
        'alertGroupNewRecipient',
        'selectedConfigGroup'
      );
      const hasRecipients = _.has(groupRecipients, 'recipients');
      // Any missing required field values?
      for (var field of requiredFields) {
        if (Ember.isBlank(this.get(field))) {
          isDisabled = true;
        }
      }
      // Enable submit if either of these field values are present
      if (Ember.isBlank(groupRecipients) && Ember.isBlank(newConfigGroupName)) {
        isDisabled = true;
      }
      // Duplicate alert Name or group name
      if (isAlertNameDuplicate || isGroupNameDuplicate) {
        isDisabled = true;
      }
      // For alert group email recipients, require presence only if group recipients is empty
      if (Ember.isBlank(alertGroupNewRecipient) && !hasRecipients) {
        isDisabled = true;
      }
      // Disable after submit clicked
      if (isProcessingForm) {
        isDisabled = true;
      }
      return isDisabled;
    }
  ),

  /**
   * Double-check new email array for errors.
   * @method isEmailValid
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean} whether errors were found
   */
  isEmailValid(emailArr) {
    const emailRegex = /^.{3,}@linkedin.com$/;
    let isValid = true;

    for (var email of emailArr) {
      if (!emailRegex.test(email)) {
        isValid = false;
      }
    }

    return isValid;
  },

  /**
   * Check for missing email address
   * @method isEmailPresent
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean}
   */
  isEmailPresent(emailArr) {
    let isPresent = true;

    if (this.get('selectedConfigGroup') || this.get('newConfigGroupName')) {
      isPresent = Ember.isPresent(this.get('selectedGroupRecipients')) || Ember.isPresent(emailArr);
    }

    return isPresent;
  },

  /**
   * Build the new alert properties based on granularity presets. This will make replay possible.
   * @method newAlertProperties
   * @param {String} alertFunctionName - new function name
   * @param {Object} selectedMetricOption - the selected metric's properties
   * @return {Object} New function object
   */
  newAlertProperties: computed(
    'alertFunctionName',
    'selectedMetricOption',
    'selectedDimension',
    'selectedFilters',
    'selectedPattern',
    'selectedGranularity',
    'selectedWeeklyEffect',
    'sensitivityWithDefault',
    function() {
      let gkey = '';
      let mergedProps = {};
      const granularity = this.get('graphConfig.granularity').toLowerCase();
      const pattern = encodeURIComponent(this.get('selectedPattern'));
      const sensitivity = encodeURIComponent(this.get('sensitivityWithDefault'));

      const {
        selectedFilters,
        selectedDimension,
        selectedWeeklyEffect: weeklyEffect
      } = this.getProperties('selectedFilters', 'selectedDimension', 'selectedWeeklyEffect');

      const settingsByGranularity = {
        common: {
          functionName: this.get('alertFunctionName') || this.get('alertFunctionName').trim(),
          metric: this.get('selectedMetricOption.name'),
          dataset: this.get('selectedMetricOption.dataset'),
          dataGranularity: this.get('selectedGranularity'),
          metricFunction: 'SUM',
          isActive: true
        },
        minute: {
          type: 'CONFIDENCE_INTERVAL_SIGN_TEST',
          windowSize: 6,
          windowUnit: 'HOURS',
          properties: 'signTestWindowSize=24;anomalyRemovalThreshold=0.6;baseline=w/3wAvg;decayRate=0.5;signTestStepSize=1;slidingWindowWidth=8;confidenceLevel=0.99'
        },
        hour: {
          type: 'REGRESSION_GAUSSIAN_SCAN',
          windowSize: 84,
          windowUnit: 'HOURS',
          windowDelay: 0,
          windowDelayUnit: 'HOURS',
          cron: '0%200%2014%201%2F1%20*%20%3F%20*',
          properties: 'metricTimezone=America/Los_Angeles;anomalyRemovalThreshold=1.0;scanMinWindowSize=1;continuumOffsetUnit=3600000;scanUseBootstrap=true;scanNumSimulations=500;scanTargetNumAnomalies=1;continuumOffsetSize=1440;scanMaxWindowSize=48;pValueThreshold=0.01;scanStepSize=1'
        },
        day: {
          type: 'SPLINE_REGRESSION_VANILLA',
          windowSize: 1,
          windowUnit: 'DAYS',
          windowDelay: 0,
          windowDelayUnit: 'DAYS',
          cron: '0%200%2014%201%2F1%20*%20%3F%20*',
          properties: `continuumOffsetSize=90;continuumOffsetUnit=86400000;pValueThreshold=0.05;applyLogTransform=true;weeklyEffectRemovedInPrediction=false;weeklyEffectModeled=${weeklyEffect}`
        }
      };

      // Set granularity types
      if (granularity.includes('minute') || granularity.includes('5-minute')) { gkey = 'minute'; }
      if (granularity.includes('hour')) { gkey = 'hour'; }
      if (granularity.includes('day')) { gkey = 'day'; }

      // Add filter and dimension choices if available
      if (Ember.isPresent(selectedFilters)) {
        settingsByGranularity.common.filters = selectedFilters;
      }
      if (Ember.isPresent(selectedDimension)) {
        settingsByGranularity.common.exploreDimension = selectedDimension;
      }

      // Append extra props to preserve in the alert record
      if (gkey) {
        settingsByGranularity[gkey].properties += `;pattern=${encodeURIComponent(pattern)};sensitivity=${encodeURIComponent(sensitivity)}`;
      }

      return Object.assign(settingsByGranularity.common, settingsByGranularity[gkey]);
    }
  ),

  /**
   * Filter all existing alert groups down to only those that are active and belong to the
   * currently selected application team.
   * @method filteredConfigGroups
   * @param {Object} selectedApplication - user-selected application object
   * @return {Array} activeGroups - filtered list of groups that are active
   */
  filteredConfigGroups: computed(
    'selectedApplication',
    function() {
      const appName = this.get('selectedApplication');
      const activeGroups = this.get('allAlertsConfigGroups').filterBy('active');
      const groupsWithAppName = activeGroups.filter(group => Ember.isPresent(group.application));

      if (Ember.isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName));
      } else {
        return activeGroups;
      }
    }
  ),

  /**
   * Sets the message text over the graph placeholder before data is loaded
   * @method graphMessageText
   * @return {String} the appropriate graph placeholder text
   */
  graphMessageText: computed(
    'isMetricDataInvalid',
    function() {
      const defaultMsg = 'Once a metric is selected, the metric replay graph will show here';
      const invalidMsg = 'Sorry, metric has no current data';
      return this.get('isMetricDataInvalid') ? invalidMsg : defaultMsg;
    }
  ),

  /**
   * Preps a mailto link containing the currently selected metric name
   * @method graphMailtoLink
   * @return {String} the URI-encoded mailto link
   */
  graphMailtoLink: computed(
    'selectedMetricOption',
    function() {
      const selectedMetric = this.get('selectedMetricOption');
      const fullMetricName = `${selectedMetric.dataset}::${selectedMetric.name}`;
      const recipient = 'ask_thirdeye@linkedin.com';
      const subject = 'TE Self-Serve Create Alert Metric Issue';
      const body = `TE Team, please look into a possible inconsistency issue with [ ${fullMetricName} ]`;
      const mailtoString = `mailto:${recipient}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
      return mailtoString;
    }
  ),

  /**
   * Returns the appropriate subtitle for selected config group monitored alerts
   * @method selectedConfigGroupSubtitle
   * @return {String} title of expandable section for selected config group
   */
  selectedConfigGroupSubtitle: computed(
    'selectedConfigGroup',
    function () {
      return `Alerts Monitored by: ${this.get('selectedConfigGroup.name')}`;
    }
  ),

  /**
   * Reset the form... clear all important fields
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      isFetchingDimensions: false,
      isDimensionFetchDone: false,
      isEmailError: false,
      isEmptyEmail: false,
      isFormDisabled: false,
      isMetricSelected: false,
      isMetricDataInvalid: false,
      isSelectMetricError: false,
      selectedMetricOption: null,
      selectedPattern: null,
      selectedGranularity: null,
      selectedSensitivity: null,
      selectedWeeklyEffect: true,
      selectedDimension: null,
      alertFunctionName: null,
      selectedAppName: null,
      selectedConfigGroup: null,
      newConfigGroupName: null,
      alertGroupNewRecipient: null,
      selectedGroupRecipients: null,
      isCreateAlertSuccess: null,
      isCreateAlertError: false,
      isProcessingForm: false,
      isCreateGroupSuccess: false,
      isReplayStatusSuccess: false,
      isReplayStarted: false,
      isReplayStatusError: false,
      isGroupNameDuplicate: false,
      isAlertNameDuplicate: false,
      graphEmailLinkProps: '',
      bsAlertBannerType: 'success',
      selectedFilters: JSON.stringify({}),
      replayStatusClass: 'te-form__banner--pending'
    });
    this.send('refreshModel');
  },

  /**
   * Actions for create alert form view
   */
  actions: {


    /**
     * Handles the primary metric selection in the alert creation
     */
    onPrimaryMetricToggle() {
      return;
    },

    /**
     * When a metric is selected, fetch its props, and send them to the graph builder
     * @method onSelectMetric
     * @param {Object} selectedObj - The selected metric
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      this.clearAll();
      this.setProperties({
        isMetricDataLoading: true,
        topDimensions: [],
        selectedMetricOption: selectedObj
      });
      this.fetchMetricData(selectedObj.id)
        .then((hash) => {
          this.setProperties(hash);
          this.setProperties({
            metricGranularityOptions: hash.granularities,
            originalDimensions: hash.dimensions
          });
          this.triggerGraphFromMetric(selectedObj);
        })
        .catch((err) => {
          this.setProperties({
            isSelectMetricError: true,
            selectMetricErrMsg: err
          });
        });
    },

    /**
     * When a filter is selected, fetch new anomaly graph data based on that filter
     * and trigger a new graph load. Also filter dimension names already selected as filters.
     * @method onSelectFilter
     * @param {Object} selectedFilters - The selected filters to apply
     * @return {undefined}
     */
    onSelectFilter(selectedFilters) {
      const selectedFilterObj = JSON.parse(selectedFilters);
      const dimensionNameSet = new Set(this.get('originalDimensions'));
      const filterNames = Object.keys(JSON.parse(selectedFilters));
      let isSelectedDimensionEqualToSelectedFilter = false;

      this.set('graphConfig.filters', selectedFilters);
      // Remove selected filters from dimension options only if filter has single entity
      for (var key of filterNames) {
        if (selectedFilterObj[key].length === 1) {
          dimensionNameSet.delete(key);
          if (key === this.get('selectedDimension')) {
            isSelectedDimensionEqualToSelectedFilter = true;
          }
        }
      }
      // Update dimension options and loader
      this.setProperties({
        dimensions: [...dimensionNameSet],
        isMetricDataLoading: true
      });
      // Do not allow selected dimension to match selected filter
      if (isSelectedDimensionEqualToSelectedFilter) {
        this.set('selectedDimension', 'All');
      }
      // Fetch new graph data with selected filters
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * When a dimension is selected, fetch new anomaly graph data based on that dimension
     * and trigger a new graph load, showing the top contributing subdimensions.
     * @method onSelectFilter
     * @param {Object} selectedDimension - The selected dimension to apply
     * @return {undefined}
     */
    onSelectDimension(selectedDimension) {
      this.setProperties({
        selectedDimension,
        isMetricDataLoading: true,
        isFetchingDimensions: true,
        isDimensionFetchDone: false
      });
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * Set our selected granularity. Trigger graph reload.
     * @method onSelectGranularity
     * @param {Object} selectedObj - The selected granularity option
     * @return {undefined}
     */
    onSelectGranularity(selectedObj) {
      this.setProperties({
        selectedGranularity: selectedObj,
        isMetricDataLoading: true
      });
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * Set our selected application name
     * @method onSelectAppName
     * @param {Object} selectedObj - The selected app name option
     * @return {undefined}
     */
    onSelectAppName(selectedObj) {
      this.setProperties({
        selectedAppName: selectedObj,
        selectedApplication: selectedObj.application
      });
    },

    /**
     * Set our selected alert configuration group. If one is selected, display editable fields
     * for that group and display the list of functions that belong to that group.
     * @method onSelectConfigGroup
     * @param {Object} selectedObj - The selected config group option
     * @return {undefined}
     */
    onSelectConfigGroup(selectedObj) {
      const emails = selectedObj.recipients || '';
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        isEmptyEmail: Ember.isEmpty(emails),
        selectedGroupRecipients: emails.split(',').filter(e => String(e).trim()).join(', ')
      });
      this.prepareFunctions(selectedObj).then(functionData => {
        this.set('selectedGroupFunctions', functionData);
      });
    },

    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} name - The new alert name
     * @return {undefined}
     */
    validateAlertName(name) {
      let isDuplicateName = false;
      this.fetchAnomalyByName(name).then(anomaly => {
        for (var resultObj of anomaly) {
          if (resultObj.functionName === name) {
            isDuplicateName = true;
          }
        }
        this.set('isAlertNameDuplicate', isDuplicateName);
      });
    },

    /**
     * Reset selected group list if user chooses to create a new group
     * @method validateNewGroupName
     * @param {String} name - User-provided alert group name
     * @return {undefined}
     */
    validateNewGroupName(name) {
      this.set('isGroupNameDuplicate', false);
      // return early if name is empty
      if (!name || !name.trim().length) { return; }
      const nameExists = this.get('allAlertsConfigGroups')
        .map(group => group.name)
        .includes(name);

      // set error message and return early if group name exists
      if (nameExists) {
        this.set('isGroupNameDuplicate', true);
        return;
      }

      this.setProperties({
        newConfigGroupName: name,
        selectedConfigGroup: null,
        selectedGroupRecipients: null,
        isEmptyEmail: Ember.isEmpty(this.get('alertGroupNewRecipient'))
      });
    },

    /**
     * Verify that email address does not already exist in alert group. If it does, remove it and alert user.
     * @method validateAlertEmail
     * @param {String} emailInput - Comma-separated list of new emails to add to the config group.
     * @return {undefined}
     */
    validateAlertEmail(emailInput) {
      const newEmailArr = emailInput.replace(/\s+/g, '').split(',');
      let existingEmailArr = this.get('selectedGroupRecipients');
      let cleanEmailArr = [];
      let badEmailArr = [];
      let isDuplicateEmail = false;

      // Release submit button error state
      this.setProperties({
        isEmailError: false,
        isProcessingForm: false,
        isEditedConfigGroup: true,
        isEmptyEmail: Ember.isPresent(this.get('newConfigGroupName')) && !emailInput.length
      });

      // Check for duplicates
      if (emailInput.trim() && existingEmailArr) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (email.length && existingEmailArr.includes(email)) {
            isDuplicateEmail = true;
            badEmailArr.push(email);
          } else {
            cleanEmailArr.push(email);
          }
        }
        this.setProperties({
          isDuplicateEmail,
          duplicateEmails: badEmailArr.join()
        });
      }
    },

    /**
     * Reset the form... clear all important fields
     * @method clearAll
     * @return {undefined}
     */
    onResetForm() {
      this.clearAll();
    },

    /**
     * Enable reaction to dimension toggling in graph legend component
     * @method onSelection
     * @return {undefined}
     */
    onSelection(selectedDimension) {
      const { isSelected } = selectedDimension;
      Ember.set(selectedDimension, 'isSelected', !isSelected);
    },

    /**
     * User hits submit... Buckle up - we're going for a ride! What we have to do here is:
     *  1. Make sure all fields are validated (done inline and with computed props)
     *  2. Disable submit button
     *  3. Send a new 'alert function' create request, which should return a new function ID
     *  4. Add this Id to the 'Alert Config Group' for notifications
     *  5. Send a Edit or Create request for the Alert Config Group based on user's choice
     *  6. Trigger metric replay (new time-based query to DB for anomaly detection tuning)
     *  7. Notify user of result
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      // This object contains the data for the new config group
      const newConfigObj = {
        active: true,
        emailConfig: { "functionIds": [] },
        recipients: this.get('alertGroupNewRecipient'),
        name: this.get('selectedConfigGroup.name') || this.get('newConfigGroupName').trim(),
        application: this.get('selectedAppName').application || null,
        cronExpression: '0 0/5 * 1/1 * ? *'
      };

      // This object contains the data for the new alert function, with default fillers
      const {
        redirectToAlertPage,
        newAlertProperties: newFunctionObj,
        selectedGroupRecipients: oldEmails,
        alertGroupNewRecipient: newEmails
      } = this.getProperties('redirectToAlertPage', 'newAlertProperties', 'selectedGroupRecipients', 'alertGroupNewRecipient');
      const newEmailsArr = newEmails ? newEmails.replace(/ /g, '').split(',') : [];
      const existingEmailsArr = oldEmails ? oldEmails.replace(/ /g, '').split(',') : [];
      const newRecipientsArr = newEmailsArr.length ? existingEmailsArr.concat(newEmailsArr) : existingEmailsArr;
      const cleanRecipientsArr = newRecipientsArr.filter(e => String(e).trim()).join(',');
      const emailError = !this.isEmailValid(newEmailsArr);

      // Are we in edit or create mode for config group?
      const isEditGroupMode = this.get('isAlertGroupEditModeActive');

      // A reference to whichever 'alert config' object will be sent. Let's default to the new one
      let finalConfigObj = newConfigObj;

      this.setProperties({
        isProcessingForm: true,
        isEmptyEmail: !this.isEmailPresent(newEmailsArr),
        isEmailError: emailError
      });

      // Exit quietly (showing warning) in the event of error
      if (emailError || this.get('isDuplicateEmail')) { return; }

      // URL encode filters to avoid API issues
      newFunctionObj.filters = encodeURIComponent(newFunctionObj.filters);

      // Add selected severity options to alert function
      newFunctionObj.alertFilter = this.get('alertFilterObj');

      // First, save our new alert function.
      this.saveThirdEyeFunction(newFunctionObj).then(newFunctionId => {

        // Add new email recipients if we are dealing with an existing Alert Group
        if (isEditGroupMode) {
          let recipientsArr = [];
          if (this.selectedConfigGroup.recipients.length) {
            recipientsArr = this.selectedConfigGroup.recipients.split(',');
          }
          recipientsArr.push(this.alertGroupNewRecipient);
          this.selectedConfigGroup.recipients = recipientsArr.join();
          finalConfigObj = this.selectedConfigGroup;
        }

        // Add our new Alert Function Id to the Alert Config Object
        finalConfigObj.emailConfig.functionIds.push(newFunctionId);

        // Finally, save our Alert Config Groupg
        this.saveThirdEyeEntity(finalConfigObj, 'ALERT_CONFIG')
          .then(alertResult => {
            // Start the replay sequence and transition to Alert Page
            this.send('triggerReplaySequence', newFunctionId);
        // If Alert Group edit/create fails, remove the orphaned anomaly Id
        }).catch((error) => {
          this.setAlertCreateErrorState(error);
          this.removeThirdEyeFunction(newFunctionId);
        });
      // Alert creation call has failed
      }).catch((error) => {
        this.setAlertCreateErrorState(error);
      });
    }
  }
});
