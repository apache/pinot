/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import fetch from 'fetch';
import Ember from 'ember';
import moment from 'moment';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';
import { task, timeout } from 'ember-concurrency';

export default Ember.Controller.extend({

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
  metricGranularityOptions: [],
  originalDimensions: [],
  bsAlertBannerType: 'success',
  graphEmailLinkProps: '',
  replayStatusClass: 'te-form__banner--pending',
  isGroupNameDuplicate: false,
  isAlertNameDuplicate: false,

  legendText: {
    dotted: 'WoW',
    solid: 'Observed'
  },

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
   * Options for sensitivity field
   */
  sensitivityOptions: ['Robust', 'Medium', 'Sensitive'],

  /**
   * Mapping user readable sensitivity to be values
   */
  sensitivityMapping: {
    Robust: 'LOW',
    Medium: 'MEDIUM',
    Sensitive: 'HIGH'
  },

  /**
   * Mapping user readable pattern to be values
   */
  patternMapping: {
    'Up and Down': 'UP,DOWN',
    'Up only': 'UP',
    'Down only': 'DOWN'
  },

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
   * Loads time-series data into the anomaly-graph component
   * @method triggerGraphFromMetric
   * @param {Number} metricId - Id of selected metric to graph
   * @return {undefined}
   */
  triggerGraphFromMetric(metricId) {
    const id = metricId.id;
    const maxTime = this.get('maxTime');
    const currentEnd = moment(maxTime).isValid()
      ? moment(maxTime).valueOf()
      : moment().subtract(1, 'day').endOf('day').valueOf();
    const filters = this.get('selectedFilters') || '';
    const dimension = this.get('selectedDimension') || 'All';
    const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
    const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(1, 'week');
    const granularity = this.get('selectedGranularity') || this.get('granularities.firstObject') || '';
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

    if (Ember.isEmpty(filters)) {
      this.set('isFilterSelectDisabled', true);
    }

    this.setProperties({
      graphConfig: graphConfig,
      selectedGranularity: granularity
    });

    this.fetchAnomalyGraphData(this.get('graphConfig')).then(metricData => {
      this.set('isMetricDataLoading', false);
      if (!this.isMetricGraphable(metricData)) {
        // Metric has no data. not graphing
        this.clearAll();
        this.set('isMetricDataInvalid', true);
      } else {
        // Metric has data. now sending new data to graph
        this.setProperties({
          isMetricSelected: true,
          selectedMetric: Object.assign(metricData, { color: 'blue' })
        });
      }
    })
    .catch((error) => {
      // The request failed. No graph to render.
      this.clearAll();
      this.setProperties({
        isSelectMetricError: true,
        isMetricDataInvalid: true,
        isMetricDataLoading: false,
        selectMetricErrMsg: error
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
   * Replay Flow Step 2 - Replay cloned function
   * @method callReplayStart
   * @param {Number} clonedId - id of the cloned function
   * @param {Object} startTime - replay start time stamp
   * @param {Object} endTime - replay end time stamp
   * @return {Ember.RSVP.Promise}
   */
  callReplayStart(functionId, startTime, endTime) {
    const granularity = this.get('graphConfig.granularity').toLowerCase();
    const speedUp = !(granularity.includes('hour') || granularity.includes('day'));
    const recipients = this.get('selectedConfigGroup.recipients');
    const selectedSensitivity = this.get('selectedSensitivity');
    const sensitivy = this.sensitivityMapping[selectedSensitivity];
    const selectedPattern = this.get('selectedPattern');
    const pattern = this.patternMapping[selectedPattern];


    const url = `/detection-job/${functionId}/notifyreplaytuning?start=${startTime}` +
      `&end=${endTime}&speedup=${speedUp}&userDefinedPattern=${pattern}&sensitivity=${sensitivy}` +
      `&removeAnomaliesInWindow=true&removeAnomaliesInWindow=true&to=${recipients}`;

    return fetch(url, { method: 'post' })
      .then((res) => checkStatus(res, 'post'))
      .catch((error) => {
        this.setProperties({
          isReplayStatusError: true,
          isReplayStatusPending: false,
          bsAlertBannerType: 'danger',
          replayStatusClass: 'te-form__banner--failure',
          failureMessage: `The replay sequence has been interrupted. (${error})`
        });
      });
  },

  /**
   * Sends a request to begin advanced replay for a metric. The replay will fetch new
   * time-series data based on user-selected sensitivity settings.
   * @method triggerReplay
   * @param {Number} newFuncId - the id for the newly created function (alert)
   * @return {Ember.RSVP.Promise}
   */
  triggerReplay(newFuncId) {
    const startTime = moment().subtract(1, 'month').endOf('day').utc().format("YYYY-MM-DDTHH:mm:ss.SSS[Z]");
    const endTime = moment().subtract(1, 'day').endOf('day').utc().format("YYYY-MM-DDTHH:mm:ss.SSS[Z]");
    const that = this;

    // Set banner to 'pending' state
    this.setProperties({
      isReplayStarted: true,
      isReplayStatusPending: true
    });

    // Begin triggering of replay sequence
    this.callReplayStart(newFuncId, startTime, endTime);

    // Simulate trigger response time since currently response takes 30+ seconds
    Ember.run.later((function() {
      if (!that.get('isReplayStatusError')) {
        that.setProperties({
          isReplayStatusSuccess: true,
          isReplayStatusPending: false,
          replayStatusClass: 'te-form__banner--success'
        });
      }
    }), 3000);
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
  isAlertGroupEditModeActive: Ember.computed(
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
  isFilterSelectDisabled: Ember.computed(
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
  isGranularitySelectDisabled: Ember.computed(
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
  isSubmitDisabled: Ember.computed(
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
    function() {
      let isDisabled = false;
      const requiredFields = this.get('requiredFields');
      const groupRecipients = this.get('selectedConfigGroup.recipients');
      // Any missing required field values?
      for (var field of requiredFields) {
        if (Ember.isBlank(this.get(field))) {
          isDisabled = true;
        }
      }
      // Enable submit if either of these field values are present
      if (Ember.isBlank(this.get('selectedConfigGroup')) && Ember.isBlank(this.get('newConfigGroupName'))) {
        isDisabled = true;
      }

      // Duplicate alert Name or group name
      if (this.get('isAlertNameDuplicate') || this.get('isGroupNameDuplicate')) {
        isDisabled = true;
      }
      // For alert group email recipients, require presence only if group recipients is empty
      if (Ember.isBlank(this.get('alertGroupNewRecipient')) && !groupRecipients) {
        isDisabled = true;
      }
      return isDisabled;
    }
  ),

  /**
   * Build the new alert properties based on granularity presets. This will make replay possible.
   * @method newAlertProperties
   * @param {String} alertFunctionName - new function name
   * @param {Object} selectedMetricOption - the selected metric's properties
   * @return {Object} New function object
   */
  newAlertProperties: Ember.computed(
    'alertFunctionName',
    'selectedMetricOption',
    'selectedDimension',
    'selectedFilters',
    'selectedGranularity',
    function() {
      let gkey = '';
      const granularity = this.get('graphConfig.granularity').toLowerCase();
      const selectedFilter = this.get('selectedFilters');
      const selectedDimension = this.get('selectedDimension');
      const weeklyEffect = this.get('selectedWeeklyEffect');
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
          type: 'SIGN_TEST_VANILLA',
          windowSize: 6,
          windowUnit: 'HOURS',
          properties: 'signTestWindowSize=24;anomalyRemovalThreshold=0.6;signTestPattern=UP,DOWN;pValueThreshold=0.01;signTestBaselineShift=0.0,0.0;signTestBaselineLift=1.10,0.90;baseline=w/4wAvg;decayRate=0.5;signTestStepSize=1'
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
          type: 'SPLINE_REGRESSION',
          windowSize: 1,
          windowUnit: 'DAYS',
          windowDelay: 0,
          windowDelayUnit: 'DAYS',
          cron: '0%200%2014%201%2F1%20*%20%3F%20*',
          properties: `pValueThreshold=0.05;logTransform=true;weeklyEffectRemovedInPrediction=false;weeklyEffectModeled=${weeklyEffect}`
        }
      };

      // Set granularity types
      if (granularity.includes('minute') || granularity.includes('5-minute')) { gkey = 'minute'; }
      if (granularity.includes('hour')) { gkey = 'hour'; }
      if (granularity.includes('day')) { gkey = 'day'; }

      // Add filter and dimension choices if available
      if (Ember.isPresent(selectedFilter)) {
        settingsByGranularity.common.filters = selectedFilter;
      }
      if (Ember.isPresent(selectedDimension)) {
        settingsByGranularity.common.exploreDimension = selectedDimension;
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
  filteredConfigGroups: Ember.computed(
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
  graphMessageText: Ember.computed(
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
  graphMailtoLink: Ember.computed(
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
  selectedConfigGroupSubtitle: Ember.computed(
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
      isFormDisabled: false,
      isMetricSelected: false,
      isMetricDataInvalid: false,
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
     * When a metric is selected, fetch its props, and send them to the graph builder
     * @method onSelectMetric
     * @param {Object} selectedObj - The selected metric
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      this.setProperties({
        isMetricDataLoading: true,
        isSelectMetricError: false,
        selectedMetricOption: selectedObj,
        selectedFilters: JSON.stringify({}),
        selectedPattern: null,
        selectedDimension: null
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
        selectedGroupRecipients: null
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

      if (emailInput.trim() && existingEmailArr) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (existingEmailArr.includes(email)) {
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
        application: this.get('selectedAppName').application || null
      };

      // This object contains the data for the new alert function, with default fillers
      const newFunctionObj = this.get('newAlertProperties');

      // Are we in edit or create mode for config group?
      const isEditGroupMode = this.get('isAlertGroupEditModeActive');

      // A reference to whichever 'alert config' object will be sent. Let's default to the new one
      let finalConfigObj = newConfigObj;

      // URL encode filters to avoid API issues
      newFunctionObj.filters = encodeURIComponent(newFunctionObj.filters);

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
        this.saveThirdEyeEntity(finalConfigObj, 'ALERT_CONFIG').then(alertResult => {
          // Display success confirmations including new alert Id and recipients
          this.setProperties({
            selectedGroupRecipients: finalConfigObj.recipients.replace(/,+/g, ', '),
            isCreateAlertSuccess: true,
            finalFunctionId: newFunctionId
          });
          // Confirm group creation if not in group edit mode
          if (!isEditGroupMode) {
            this.set('isCreateGroupSuccess', true);
          }
          // Display function added to group confirmation
          this.prepareFunctions(finalConfigObj, newFunctionId).then(functionData => {
            this.set('selectedGroupFunctions', functionData);
          });
          // Trigger alert replay.
          this.triggerReplay(newFunctionId);
          // Now, disable form
          this.setProperties({
            isFormDisabled: true,
            isMetricSelected: false,
            isMetricDataInvalid: false
          });
          return newFunctionId;
        })
        // Todo: redirects once edit page is finalized
        // .then((id) => {
        //   this.transitionToRoute('manage.alerts.edit', id);
        // })
        // If Alert Group edit/create fails, remove the orphaned anomaly Id
        .catch((error) => {
          this.setAlertCreateErrorState(error);
          this.removeThirdEyeFunction(newFunctionId);
        });
      })
      // Alert creation call has failed
      .catch((error) => {
        this.setAlertCreateErrorState(error);
      });
    }
  }
});
