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
  isSubmitDisabled: true,
  isCreateAlertSuccess: false,
  isCreateGroupSuccess: false,
  isCreateAlertError: false,
  isReplayComplete: false,
  isReplayTriggeredSuccess: false,
  metricGranularityOptions: [],

  /**
   * Component property initial settings
   */
  filters: {},
  graphConfig: {},
  filterPropNames: JSON.stringify({}),

  /**
   * Object to cover basic ield 'presence' validation
   */
  requiredFields: [
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName',
    'alertGroupNewRecipient'],

  /**
   * Options for patterns of interest field. These may eventually load from the backend.
   */
  patternsOfInterest: ['Up and Down', 'Up only', 'Down only'],

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
    return fetch(url).then(res => res.json());
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
   * Fetches the selected metric's dimension data. TODO: Set up custom response handler for HTTP errors.
   * See: https://github.com/github/fetch#handling-http-error-statuses
   * @method fetchMetricDimensions
   * @param {Number} metricId - Id for the selected metric
   * @return {Promise}
   */
  fetchMetricDimensions(metricId) {
    const url = `/data/autocomplete/dimensions/metric/${metricId}`;
    return fetch(url).then(res => res.json());
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
    return fetch(url).then(res => res.json());
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
    return fetch(url).then(res => res.json());
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * @method fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {Ember.RSVP.promise}
   */
  fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => res.json()),
      granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => res.json()),
      filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => res.json()),
      selectedMetricDimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res =>res.json())
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
    this.set('loading', true);

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

    const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}&filters=${filters}`;

    return fetch(url).then(res => res.json());
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
    return fetch(url, postProps);
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
    return fetch(url, postProps).then(res => res.json());
  },

  /**
   * Loads time-series data into the anomaly-graph component
   * @method triggerGraphFromMetric
   * @param {Number} metricId - Id of selected metric to graph
   * @return {undefined}
   */
  triggerGraphFromMetric(metricId) {
    const maxTime = this.get('maxTime');
    const currentEnd = moment(maxTime).isValid()
      ? moment(maxTime).valueOf()
      : moment().subtract(1, 'day').endOf('day').valueOf();
    const granularity = this.get('selectedGranularity') || this.get('granularities.firstObject');
    const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
    const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(1, 'week');
    const { id } = metricId;

    const graphConfig = {
      id,
      dimension: 'All',
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity
    };

    this.setProperties({
      graphConfig: graphConfig,
      selectedGranularity: granularity
    });

    this.fetchAnomalyGraphData(this.get('graphConfig')).then(metricData => {
      this.setProperties({
        isMetricSelected: true,
        selectedMetric: metricData,
        loading: false
      });
    });
  },

  /**
   * Sends a request to begin advanced replay for a metric. The replay will fetch new
   * time-series data based on user-selected sensitivity settings.
   * @method triggerReplay
   * @param {Object} functionObj - the alert object
   * @param {Object} groupObj - the alert notification group object
   * @param {Number} newFuncId - the id for the newly created function (alert)
   * @return {Ember.RSVP.Promise}
   */
  triggerReplay(functionObj, groupObj, newFuncId) {
    const startTime = moment().subtract(1, 'month').endOf('day').format("YYYY-MM-DD");
    const startStamp = moment().subtract(1, 'day').endOf('day').valueOf();
    const endTime = moment().subtract(1, 'day').endOf('day').format("YYYY-MM-DD");
    const endStamp = moment().subtract(1, 'month').endOf('day').valueOf();
    const granularity = this.get('graphConfig.granularity').toLowerCase();
    const postProps = {
      method: 'POST',
      timeout: 120000, // 2 min
      headers: { 'Content-Type': 'Application/Json' }
    };
    const replayApi = {
      minute: `/replay/singlefunction?functionId=${newFuncId}&start=${startTime}&end=${endTime}`,
      hour: `/${newFuncId}/replay?start=${startTime}&end=${endTime}`,
      day: `/replay/function/${newFuncId}?start=${startTime}&end=${endTime}&goal=1.0&evalMethod=F1_SCORE&includeOriginal=false&tune=\{"pValueThreshold":\[0.001,0.005,0.01,0.05\]\}`,
      reports: `/thirdeye/email/generate/metrics/${startStamp}/${endStamp}?metrics=${functionObj.metric}&subject=Your%20Metric%20Has%20Onboarded%20To%20Thirdeye&from=thirdeye-noreply@linkedin.com&to=${groupObj.recipients}&smtpHost=email.corp.linkedin.com&smtpPort=25&includeSentAnomaliesOnly=true&isApplyFilter=true`
    };
    const allowedGranularity = ['minute', 'hour', 'day'];
    const gkey = allowedGranularity.includes(granularity) ? granularity : 'minute';

    return new Ember.RSVP.Promise((resolve) => {
      fetch('/detection-job' + replayApi[gkey], postProps).then(res => resolve(checkStatus(res)));
    });
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
    return new Ember.RSVP.Promise((resolve) => {
      for (var functionId of existingFunctionList) {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push({
            id: functionData.id,
            name: functionData.functionName,
            metric: functionData.metric + '::' + functionData.collection,
            type: functionData.type,
            active: functionData.isActive,
            isNewId: functionData.id === newId
          });
          cnt ++;
          if (existingFunctionList.length === cnt) {
            resolve(newFunctionList);
          }
        });
      }
    });
  },

  /**
   * If these two conditions are true, we assume the user wants to edit an existing alert group
   * @method isSubmitDisabled
   * @param {Number} metricId - Id of selected metric to graph
   * @return {Boolean} PreventSubmit
   */
  isAlertGroupEditModeActive: Ember.computed(
    'selectedConfigGroup',
    'newConfigGroupName',
    function() {
      return this.get('selectedConfigGroup') && Ember.isNone(this.get('newConfigGroupName'));
    }),

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @param {Number} metricId - Id of selected metric to graph
   * @return {Boolean} PreventSubmit
   */
  isSubmitDisabled: Ember.computed(
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    function() {
      const requiredFields = this.get('requiredFields');
      let isDisabled = false;
      // Any missing required field values?
      for (var field of requiredFields) {
        if (Ember.isNone(this.get(field))) {
          isDisabled = true;
        }
      }
      // Enable submit if either of these field values are present
      if (Ember.isNone(this.get('selectedConfigGroup')) && Ember.isNone(this.get('newConfigGroupName'))) {
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
    function() {
      let gkey = '';
      const granularity = this.get('graphConfig.granularity').toLowerCase();
      const settingsByGranularity = {
        common: {
          functionName: this.get('alertFunctionName'),
          metric: this.get('selectedMetricOption.name'),
          dataset: this.get('selectedMetricOption.dataset'),
          metricFunction: 'SUM',
          isActive: true
        },
        minute: {
          type: 'SIGN_TEST_VANILLA',
          windowSize: 6,
          windowUnit: 'HOURS',
          properties: 'signTestWindowSize=24;anomalyRemovalWeightThreshold=0.6;signTestPattern=UP,DOWN;pValueThreshold=0.01;signTestBaselineShift=0.0,0.0;signTestBaselineLift=1.10,0.90;baseline=w/4wAvg;decayRate=0.5;signTestStepSize=1'
        },
        hour: {
          type: 'REGRESSION_GAUSSIAN_SCAN',
          windowSize: 84,
          windowUnit: 'HOURS',
          windowDelay: 8,
          windowDelayUnit: 'HOURS',
          cron: '0%200%2014%201%2F1%20*%20%3F%20*',
          properties: 'metricTimezone=America/Los_Angeles;anomalyRemovalWeightThreshold=1.0;scanMinWindowSize=1;continuumOffsetUnit=3600000;scanUseBootstrap=true;scanNumSimulations=500;scanTargetNumAnomalies=1;continuumOffsetSize=1440;scanMaxWindowSize=48;pValueThreshold=0.01;scanStepSize=1'
        },
        day: {
          type: 'SPLINE_REGRESSION',
          windowSize: 1,
          windowUnit: 'DAYS',
          windowDelay: 0,
          windowDelayUnit: 'DAYS',
          cron: '0%200%2014%201%2F1%20*%20%3F%20*',
          properties: 'pValueThreshold=0.05;logTransform=true;weeklyEffectRemovedInPrediction=false'
        }
      };

      if (granularity.includes('minute') || granularity.includes('5-minute')) { gkey = 'minute'; }
      if (granularity.includes('hour')) { gkey = 'hour'; }
      if (granularity.includes('day')) { gkey = 'day'; }

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
      this.set('loading', true);
      this.set('selectedMetricOption', selectedObj);
      this.fetchMetricData(selectedObj.id).then((hash) => {
        this.setProperties(hash);
        this.set('metricGranularityOptions', hash.granularities);
        this.triggerGraphFromMetric(selectedObj);
      });
    },

    /**
     * When a filter is selected, fetch new anomaly graph data based on that filter
     * and trigger 'onSelectedMetric' to load the graph again.
     * @method onSelectFilter
     * @param {Object} filters - The selected filter to apply
     * @return {undefined}
     */
    onSelectFilter(filters) {
      this.set('graphConfig.filters', filters);
      this.fetchAnomalyGraphData(this.get('graphConfig')).then(metricData => {
        this.setProperties({
          isMetricSelected: true,
          selectedMetric: metricData,
          loading: false
        });
      });
    },

    /**
     * Set our selected granularity. Trigger graph reload.
     * @method onSelectGranularity
     * @param {Object} selectedObj - The selected granularity option
     * @return {undefined}
     */
    onSelectGranularity(selectedObj) {
      this.set('selectedGranularity', selectedObj);
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
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        selectedGroupRecipients: selectedObj.recipients.replace(/,+/g, ', ')
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
      let isDuplicateErr = false;

      if (emailInput.trim() && existingEmailArr) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (existingEmailArr.includes(email)) {
            isDuplicateErr = true;
            badEmailArr.push(email);
          } else {
            cleanEmailArr.push(email);
          }
        }

        this.setProperties({
          isDuplicateEmail: isDuplicateErr,
          duplicateEmails: badEmailArr.join()
        });
      }
    },

    /**
     * Reset the form... clear all important fields
     * @method clearAll
     * @return {undefined}
     */
    clearAll() {
      this.setProperties({
        isFormDisabled: false,
        isMetricSelected: false,
        selectedMetricOption: null,
        selectedPattern: null,
        selectedGranularity: null,
        dimensionSelectorVal: null,
        alertFunctionName: null,
        selectedAppName: null,
        selectedConfigGroup: null,
        newConfigGroupName: null,
        alertGroupNewRecipient: null,
        selectedGroupRecipients: null,
        isCreateAlertSuccess: null,
        isCreateAlertError: false,
        isCreateGroupSuccess: false,
        isReplayTriggeredSuccess: false,
        isReplayComplete: false,
        filterPropNames: JSON.stringify({})
      });
      this.send('refreshModel');
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
        name: this.get('selectedConfigGroup') || this.get('newConfigGroupName'),
        application: this.get('selectedAppName').application || null
      };

      // This object contains the data for the new alert function, with default fillers
      const newFunctionObj = this.get('newAlertProperties');

      // Are we in edit or create mode for config group?
      const isEditGroupMode = this.get('isAlertGroupEditModeActive');

      // A reference to whichever 'alert config' object will be sent. Let's default to the new one
      let finalConfigObj = newConfigObj;

      // First, save our new alert function. TODO: deal with request failure case.
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

        // Proceed only if function creation succeeds and returns an ID
        if (Ember.typeOf(newFunctionId) === 'number') {
          // Add our new Alert Function Id to the Alert Config Object
          finalConfigObj.emailConfig.functionIds.push(newFunctionId);
          // Finally, save our Alert Config Group
          this.saveThirdEyeEntity(finalConfigObj, 'ALERT_CONFIG').then(alertResult => {
            if (alertResult.ok) {
              this.setProperties({
                selectedGroupRecipients: finalConfigObj.recipients,
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
              // Display alert trigger confirmation
              this.set('isReplayTriggeredSuccess', true);
              // Trigger alert replay. TODO: only set 'complete' if response is good
              this.triggerReplay(newFunctionObj, finalConfigObj, newFunctionId).then(result => {
                this.set('isReplayComplete', true);
              });
              // Now, disable form
              this.setProperties({
                isFormDisabled: true,
                isMetricSelected: false
              });
            } else {
              this.set('isCreateAlertError', true);
            }
          });
        } else {
          this.set('isCreateAlertError', true);
        }
      });
    }
  }
});
