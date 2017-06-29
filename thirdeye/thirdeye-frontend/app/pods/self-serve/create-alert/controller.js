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
  filters: {},
  graphConfig: {},

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
    const url = `/data/autocomplete/dimensions/metric/${metricId}`;
    return fetch(url)
      .then(res => res.json())
  },

  fetchFunctionById(functionId) {
    const url = `/onboard/function/${functionId}`;
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

  prepareFunctions: function(configGroup) {
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
            active: functionData.isActive
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
      this.fetchMetricData(selectedObj.id).then((hash) => {
        this.setProperties(hash);
        this.triggerGraphFromMetric(selectedObj);
      })
      // this.loadDimensionOptions(selectedObj);
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
          let recipientsArr = [];
          if (this.selectedConfigGroup.recipients.length) {
            recipientsArr = this.selectedConfigGroup.recipients.split(',');
          }
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
