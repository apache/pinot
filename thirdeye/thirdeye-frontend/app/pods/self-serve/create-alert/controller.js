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

  init() {
    this._super(...arguments);
    this.set('isSubmitDisabled', true);
  },

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
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  fetchMetricDimensions(metricId) {
    const url = `data/autocomplete/dimensions/metric/${metricId}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchMetricMaxTime(metricId) {
    const url = `/data/maxDataTime/metricId/${metricId}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchAnomalyGraphData(config) {
    console.log(config);
    const url = `/timeseries/compare/${config.id}/${config.currentStart}/${config.maxTime}/${config.baselineStart}/${config.baselineEnd}?dimension=${config.dimension}&granularity=MINUTES&filters={}`;
    return fetch(url)
      .then(res => res.json())
  },

  triggerGraphFromMetric(metric) {
    this.fetchMetricMaxTime(metric.id, 'All').then(maxTime => {
      console.log('maxTime: ', maxTime);
      const fixedMaxTime = maxTime || moment().subtract(1,'day').endOf('day').valueOf();
      const currentStart = moment(fixedMaxTime).subtract(3, 'days');
      const baselineStart = currentStart.clone().subtract(1, 'week');
      this.graphConfig = {
        id: metric.id,
        dimension: 'All',
        maxTime: fixedMaxTime,
        currentStart: currentStart.valueOf(),
        baselineStart: baselineStart.valueOf(),
        baselineEnd: moment(fixedMaxTime).subtract(1, 'week').valueOf()
      };
      this.fetchAnomalyGraphData(this.graphConfig).then(metricData => {
        this.set('isMetricSelected', true);
        this.set('selectedMetric', metricData);
      });
    });
  },

  triggerGraphFromDimensions(dimension) {
    this.graphConfig.dimension = dimension;
    this.fetchAnomalyGraphData(this.graphConfig).then(metricData => {
      this.set('isMetricSelected', true);
      this.set('selectedMetric', metricData);
    });
  },

  loadDimensionOptions(metric) {
    this.fetchMetricDimensions(metric.id).then(dimensionData => {
      this.set('selectedMetricDimensions', dimensionData || []);
    });
  },

  prepareFunctions(configGroup) {
    const newFunctionList = [];
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    for (var functionId of existingFunctionList) {
      console.log(functionId);
      newFunctionList.push({
        id: functionId,
        name: 'some name'
      });
    }
    return newFunctionList;
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
    const postProps = {
      method: 'post',
      headers: { 'content-type': 'Application/Json' }
    };
    const url = '/dashboard/anomaly-function/create?' + $.param(functionData);
    return fetch(url, postProps)
      .then(res => res.json());
  },

  checkRequiredValues() {
    if (this.get('selectedMetricOption') && this.get('selectedPattern') && this.get('alertFunctionName') && this.get('alertGroupNewRecipient')) {
      return false;
    } else {
      return true;
    }
  },

  /**
   * Placeholder for patterns of interest options
   */
  patternsOfInterest: ['None', 'Up', 'Down', 'Either'],
  /**
   * Placeholder for alert groups options
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allAlertsConfigGroups'),

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
      this.triggerGraphFromMetric(selectedObj);
      this.loadDimensionOptions(selectedObj);
    },

    onSelectDimension(selectedObj) {
      this.set('dimensionSelectorVal', selectedObj);
      this.triggerGraphFromDimensions(selectedObj);
    },

    onSelectPattern(selectedObj) {
      this.set('selectedPattern', selectedObj);
    },

    onSelectConfigGroup(selectedObj) {
      if (selectedObj) {
        this.set('selectedConfigGroup', selectedObj);
        this.set('selectedGroupRecipients', selectedObj.recipients);
        this.set('selectedGroupActive', selectedObj.active);
        this.set('selectedGroupFunctions', this.prepareFunctions(selectedObj));
        this.set('showAlertGroupEdit', true);
      } else {
        this.set('configSelectorVal', '');
      }
    },

    onClickChangeGroupEditMode() {
      this.toggleProperty('showAlertGroupEdit');
    },

    validateRequiredFields() {
      console.log('validating: ', this.checkRequiredValues());
      this.set('isSubmitDisabled', this.checkRequiredValues());
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
        active: this.get('createGroupActive') || false,
        emailConfig: { "functionIds": [] },
        recipients: this.get('alertGroupNewRecipient')
      };
      // This object contains the data for the new alert function, with default fillers
      const newFunctionObj = {
        functionName: this.get('alertFunctionName'),
        metric: this.get('selectedMetricOption').name,
        dataset: this.get('selectedMetricOption').dataset,
        metricFunction: 'SUM',
        type: 'SIGN_TEST_VANILLA',
        windowSize: 6,
        windowUnit: 'HOURS',
        isActive: false,
        properties: 'signTestWindowSize=24;anomalyRemovalWeightThreshold=0.6;signTestPattern=' + this.get('selectedPattern') + 'UP;pValueThreshold=0.01;signTestBaselineShift=0.0;signTestBaselineLift=0.90;baseline=w/4wAvg;decayRate=0.5;signTestStepSize=1'
      };

      // If these two conditions are true, we assume the user wants to edit an existing alert group
      const isAlertGroupEditModeActive = this.get('showAlertGroupEdit') && this.selectedConfigGroup;
      // A reference to whichever 'alert config' object will be sent. Let's default to the new one
      let finalConfigObj = newConfigObj;

      // First, save our new alert function
      this.saveThirdEyeFunction(newFunctionObj).then(functionResult => {
        console.log('saving new function', newFunctionObj);
        // Add our new Alert Function Id to the Alert Config Object
        if (Ember.typeOf(functionResult) === 'number') {
          finalConfigObj.emailConfig.functionIds.push(functionResult);
        }

        // Add new email recipients if we are dealing with an existing Alert Group
        if (isAlertGroupEditModeActive) {
          let recipientsArr = this.selectedConfigGroup.recipients.split(',');
          recipientsArr.push(this.alertGroupNewRecipient);
          this.selectedConfigGroup.recipients = recipientsArr.join();
          finalConfigObj = this.selectedConfigGroup;
        }

        // Finally, save our Alert Config Group
        this.saveThirdEyeEntity(finalConfigObj, 'ALERT_CONFIG').then(alertResult => {
          console.log('saving alert config', newFunctionObj);
          if (alertResult.ok) {
            this.set('selectedGroupRecipients', finalConfigObj.recipients);
            this.set('isCreateSuccess', true);
            this.set('finalFunctionId', functionResult);
          }
        });
      });
    }
  }
});
