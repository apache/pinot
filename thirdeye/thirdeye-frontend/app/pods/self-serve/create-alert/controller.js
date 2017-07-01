/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import moment from 'moment';
import fetch from 'fetch';

export default Ember.Controller.extend({
  /**
   * Array of metrics we're displaying
   */
  isMetricSelected: false,
  isValidated: false,
  showAlertGroupEdit: true,
  isSubmitDisabled: true,
  filters: {},
  filterPropNames: JSON.stringify({}),
  graphConfig: {},

  /**
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url)
      .then(res => res.json())
  }),

  /**
   * Pseudo-encodes the querystring params to be posted. NOTE: URI encoders cannot be used
   * here because they will encode the 'cron' property value, which causes the request to fail.
   * Previously tried ${encodeURI(key)}=${encodeURI(paramsObj[key])}
   * @method toQueryString
   * @param {paramsObj} object - the object we are flattening and url-encoding
   * @return {String}
   */
  toQueryString: function(paramsObj) {
    return Object
      .keys(paramsObj)
      .map(key => `${key}=${paramsObj[key]}`)
      .join('&');
  },

  /**
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  fetchMetricDimensions(metricId) {
    const url = `/data/autocomplete/dimensions/metric/${metricId}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchFunctionById(functionId) {
    const url = `/onboard/function/${functionId}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchAnomalyByName(name) {
    const url = `/data/autocomplete/functionByName?name=${name}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchMetricData(metricId) {
    const promiseHash = {
      granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => res.json()),
      filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => res.json()),
      maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => res.json()),
      selectedMetricDimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res =>res.json()),
    };

    return Ember.RSVP.hash(promiseHash);
  },

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
    return fetch(url)
      .then(res => res.json())
  },

  triggerGraphFromMetric(metric) {
    const maxTime = this.get('maxTime');
    const currentEnd = moment(maxTime).isValid()
      ? moment(maxTime).valueOf()
      : moment().subtract(1,'day').endOf('day').valueOf();
    const granularity = this.get('granularities.firstObject');

    const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
    const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(1, 'week');
    const { id } = metric;

    const graphConfig = {
      id,
      dimension: 'All',
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity,
    };
    this.set('graphConfig', graphConfig);
    console.log(JSON.stringify(graphConfig));

    this.fetchAnomalyGraphData(this.get('graphConfig')).then(metricData => {
      this.set('isMetricSelected', true);
      this.set('selectedMetric', metricData);
      this.set('loading', false);
    });
  },

  triggerGraphFromDimensions(dimension) {
    this.graphConfig.dimension = dimension;
    this.fetchAnomalyGraphData(this.graphConfig).then(metricData => {
      this.set('isMetricSelected', true);
      this.set('selectedMetric', metricData);
      this.set('loading', false);
    });
  },

  triggerReplay(functionObj, groupObj, newFuncId) {
    console.log('in trigger replay');
    const startTime = moment().subtract(1,'month').endOf('day').format("YYYY-MM-DD");
    const startStamp = moment().subtract(1,'day').endOf('day').valueOf();
    const endTime = moment().subtract(1,'day').endOf('day').format("YYYY-MM-DD");
    const endStamp = moment().subtract(1,'month').endOf('day').valueOf();
    const granularity = this.get('graphConfig.granularity').toLowerCase();
    const postProps = {
      method: 'POST',
      headers: { 'Content-Type': 'Application/Json' }
    };
    let gkey = '';

    const replayApi = {
      base: '/api/detection-job',
      minute: `/replay/singlefunction?functionId=${newFuncId}&start=${startTime}&end=${endTime}`,
      hour: `/${newFuncId}/replay?start=${startTime}&end=${endTime}`,
      day: `/replay/function/${newFuncId}?start=${startTime}&end=${endTime}&goal=1.0&evalMethod=F1_SCORE&includeOriginal=false&tune=\{"pValueThreshold":\[0.001,0.005,0.01,0.05\]\}`,
      reports: `/thirdeye/email/generate/metrics/${startStamp}/${endStamp}?metrics=${functionObj.metric}&subject=Your%20Metric%20Has%20Onboarded%20To%20Thirdeye&from=thirdeye-noreply@linkedin.com&to=${groupObj.recipients}&smtpHost=email.corp.linkedin.com&smtpPort=25&includeSentAnomaliesOnly=true&isApplyFilter=true`
    };

    if (granularity.includes('minute')) { gkey = 'minute'; }
    if (granularity.includes('hour')) { gkey = 'hour'; }
    if (granularity.includes('day')) { gkey = 'day'; }

    return new Ember.RSVP.Promise((resolve) => {
      fetch(replayApi.base + replayApi[gkey], postProps).then(res => resolve(res.json()));
    });
  },

  newAlertProperties: Ember.computed(
    'alertFunctionName',
    'selectedMetricOption',
    function() {
      let gkey = '';
      const granularity = this.get('graphConfig.granularity').toLowerCase();
      const settingsByGranularity = {
        common: {
          functionName: this.get('alertFunctionName'),
          metric: this.get('selectedMetricOption').name,
          dataset: this.get('selectedMetricOption').dataset,
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

      if (granularity.includes('minute')) { gkey = 'minute'; }
      if (granularity.includes('hour')) { gkey = 'hour'; }
      if (granularity.includes('day')) { gkey = 'day'; }

      return Object.assign(settingsByGranularity.common, settingsByGranularity[gkey]);
    }
  ),

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
            metric: functionData.metric,
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

  saveThirdEyeEntity(alertData, entityType) {
    const postProps = {
      method: 'post',
      body: JSON.stringify(alertData),
      headers: { 'content-type': 'Application/Json' }
    };
    const url = '/thirdeye/entity?entityType=' + entityType;
    return fetch(url, postProps);
  },

  saveThirdEyeFunction(functionData) {
    const url = '/dashboard/anomaly-function/create?' + this.toQueryString(functionData);
    const postProps = {
      method: 'post',
      headers: { 'content-type': 'Application/Json' }
    };

    return fetch(url, postProps)
      .then(res => res.json());
  },

  /**
   * Placeholder for patterns of interest options
   */
  patternsOfInterest: ['None', 'Up', 'Down', 'Either'],
  /**
   * Placeholder for alert groups options
   */
  filteredConfigGroups: Ember.computed(
    'model',
    'selectedApplication',
    function() {
      const appName = this.get('selectedApplication');
      const allAlertsConfigGroups = this.get('model.allAlertsConfigGroups');
      const activeGroups = allAlertsConfigGroups.filter(group => group.active);
      const groupsWithAppName = activeGroups.filter(group => Ember.isPresent(group.application));

      if (Ember.isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName));
      } else {
        return activeGroups;
      }
    }
  ),

  /**
   * Placeholder for app name options
   */
  allApplicationNames: Ember.computed.reads('model.allAppNames'),
  /**
   * Actions for create alert form view
   */
  actions: {
    /**
     * Function called when the dropdown value is updated
     * @method onChangeDropdown
     * @param {Object} selectedObj - If has dataset, this is the selected value from dropdown
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      console.log(selectedObj);
      this.set('selectedMetricOption', selectedObj);
      this.fetchMetricData(selectedObj.id).then((hash) => {
        this.setProperties(hash);
        this.triggerGraphFromMetric(selectedObj);
        console.log('funcObj settings : ', this.get('newAlertProperties'));
      })
    },

    onSelectFilter(filters) {
      this.set('graphConfig.filters', filters);
      this.fetchAnomalyGraphData(this.get('graphConfig')).then(metricData => {
        this.set('isMetricSelected', true);
        this.set('selectedMetric', metricData);
        this.set('loading', false);
      });
    },

    onSelectDimension(selectedObj) {
      this.set('dimensionSelectorVal', selectedObj);
    },

    onSelectPattern(selectedObj) {
      this.set('selectedPattern', selectedObj);
    },

    onSelectAppName(selectedObj) {
      this.set('selectedAppName', selectedObj);
      this.set('selectedApplication', selectedObj.application);
      this.set('alertGroupNewRecipient', selectedObj.recipients);
    },

    onSelectConfigGroup(selectedObj) {
      if (selectedObj) {
        this.set('selectedConfigGroup', selectedObj);
        this.set('selectedGroupRecipients', selectedObj.recipients.replace(/,+/g, ', '));
        this.set('showAlertGroupEdit', true);
        this.prepareFunctions(selectedObj).then(functionData => {
          this.set('selectedGroupFunctions', functionData);
        });
      } else {
        this.set('configSelectorVal', '');
      }
    },

    onClickChangeGroupEditMode() {
      this.toggleProperty('showAlertGroupEdit');
    },

    // Make sure alert name does not exist in system
    validateAlertName(name) {
      let isDuplicateName = false;
      this.fetchAnomalyByName(name).then(anomaly => {
        for (var resultObj of anomaly) {
          if (resultObj.functionName === name) {
            isDuplicateName = true;
          }
        }
        this.set('isAlertNameDuplicate', isDuplicateName);
        console.log('funcObj settings : ', this.get('newAlertProperties'));
      });
    },

    // Verify that email address does not already exist in alert group. If it does, remove it and alert user.
    validateAlertEmail(emailInput) {
      const newEmailArr = emailInput.replace(/\s+/g, '').split(',');
      let existingEmailArr = this.get('selectedGroupRecipients');
      let cleanEmailArr = [];
      let badEmailArr = [];
      let isDuplicateErr = false;

      if (emailInput.trim()) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (existingEmailArr.includes(email)) {
            isDuplicateErr = true;
            badEmailArr.push(email);
          } else {
            cleanEmailArr.push(email);
          }
        }

        this.send('validateRequired');
        this.set('isDuplicateEmail', isDuplicateErr);
        this.set('duplicateEmails', badEmailArr.join());
      }
    },

    // Ensures presence of values for these fields. TODO: make this more efficient
    validateRequired() {
      const reqFields = [
        'selectedMetricOption',
        'selectedPattern',
        'alertFunctionName',
        'selectedAppName'
      ];
      let allFieldsReady = true;

      for (var field of reqFields) {
        if (Ember.isNone(this.get(field))) {
          allFieldsReady = false;
        }
      }

      this.set('isSubmitDisabled', !allFieldsReady);
    },

    clearAll() {
      this.set('isMetricSelected', false);
      this.set('selectedMetricOption', null);
      this.set('selectedPattern', null);
      this.set('dimensionSelectorVal', null);
      this.set('alertFunctionName', null);
      this.set('selectedAppName', null);
      this.set('selectedConfigGroup', null);
      this.set('alertGroupNewRecipient', null);
      this.set('createGroupName', null);
      this.set('showAlertGroupEdit', false);
      this.set('isCreateSuccess', false);
      this.set('isCreateError', false);
      this.set('filterPropNames', JSON.stringify({}));
    },

    /**
     * User hit submit. Buckle up - we're going for a ride! What we have to do here is:
     * 1. Make sure all fields are validated
     * 2. Send a new 'alert function' create request, which should return a new function ID
     * 3. Add this Id to the 'Alert Config Group' for notifications
     * 4. Send a Edit or Create request for the Alert Config Group based on user's choice
     * 5. Notify user of result
     */
    submit() {
      // This object contains the data for the new config group
      const newConfigObj = {
        name: this.get('createGroupName'),
        active: true,
        application: this.get('selectedAppName').application || null,
        emailConfig: { "functionIds": [] },
        recipients: this.get('alertGroupNewRecipient')
      };
      // This object contains the data for the new alert function, with default fillers
      const newFunctionObj = this.get('newAlertProperties');
      // If these two conditions are true, we assume the user wants to edit an existing alert group
      const isAlertGroupEditModeActive = this.get('showAlertGroupEdit') && this.selectedConfigGroup;
      // A reference to whichever 'alert config' object will be sent. Let's default to the new one
      let finalConfigObj = newConfigObj;
      let newFunctionId = 0;

      // Disable submit
      this.set('isSubmitDisabled', true);

      // First, save our new alert function
      this.saveThirdEyeFunction(newFunctionObj).then(functionResult => {

        // Add new email recipients if we are dealing with an existing Alert Group
        if (isAlertGroupEditModeActive) {
          let recipientsArr = [];
          if (this.selectedConfigGroup.recipients.length) {
            recipientsArr = this.selectedConfigGroup.recipients.split(',');
          }
          recipientsArr.push(this.alertGroupNewRecipient);
          this.selectedConfigGroup.recipients = recipientsArr.join();
          finalConfigObj = this.selectedConfigGroup;
        }
        // Proceed only if function creation succeeds and returns an ID
        if (Ember.typeOf(functionResult) === 'number') {
          // Add our new Alert Function Id to the Alert Config Object
          newFunctionId = functionResult;
          finalConfigObj.emailConfig.functionIds.push(newFunctionId);

          // Finally, save our Alert Config Group
          this.saveThirdEyeEntity(finalConfigObj, 'ALERT_CONFIG').then(alertResult => {
            if (alertResult.ok) {
              this.set('selectedGroupRecipients', finalConfigObj.recipients);
              this.set('isCreateSuccess', true);
              this.set('finalFunctionId', functionResult);
              this.prepareFunctions(finalConfigObj, newFunctionId).then(functionData => {
                this.set('selectedGroupFunctions', functionData);
              });
              this.triggerReplay(newFunctionObj, finalConfigObj, newFunctionId).then(result => {
                console.log('done with replay : ', result);
                this.set('replayStatus', result);
              });
            }
          });
        } else {
          this.set('isCreateError', true);
        }
      });
    }
  }
});
