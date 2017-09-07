/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import fetch from 'fetch';
import Ember from 'ember';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Controller.extend({
  // Property from model
  // using 'reads' so that changes are cancellable
  metricName: Ember.computed.reads('model.metric'),
  granularity: Ember.computed.reads('model.bucketUnit'),
  dimensions: Ember.computed.reads('model.exploreDimensions'),
  alertFilter: Ember.computed.reads('model.alertFilter'),
  alertFunctionName: Ember.computed.reads('model.functionName'),
  isActive: Ember.computed.reads('model.isActive'),
  selectedAppName: Ember.computed.reads('model.application'),
  metricData: Ember.computed.reads('model.metricData'),
  subscriptionGroup: Ember.computed.reads('model.subscriptionGroup'),
  filters: Ember.computed.reads('model.filters'),
  selectedApplication: Ember.computed.reads('model.subscriptionGroup.application'),

  /**
   * Mapping alertFilter's pattern to human readable strings
   * @returns {String}
   */
  pattern: Ember.computed('alertFilter.pattern', function() {
    const pattern = this.getWithDefault('alertFilter.pattern', 'UP,DOWN');

    const patternMapping = {
      'UP,DOWN': 'Up and Down',
      UP: 'Up',
      DOWN: 'Down'
    };

    return patternMapping[pattern];
  }),

  /**
   * Extracting Weekly Effect from alert Filter
   * @returns {String}
   */
  weeklyEffect: Ember.computed('alertFilter.weeklyEffectModeled', function() {
    const weeklyEffect = this.getWithDefault('alertFilter.weeklyEffectModeled', true);

    return weeklyEffect;
  }),

    /**
   * Extracting sensitivity from alert Filter and maps it to human readable values
   * @returns {String}
   */
  sensitivity: Ember.computed('alertFilter.userDefinedPattern', function() {
    const sensitivity = this.getWithDefault('alertFilter.userDefinedPattern', 'MEDIUM');
    const sensitivityMapping = {
      LOW: 'Robust',
      MEDIUM: 'Medium',
      HIGHT: 'Sensitive'
    };

    return sensitivityMapping[sensitivity];
  }),

  /**
   * Default text value for the anomaly graph legend
   */
  legendText: {
    dotted: 'WoW',
    solid: 'Observed'
  },

  /**
   * The list of all existing alert configuration groups.
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allConfigGroups'),

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
   * Actions for edit alert form view
   */
  actions: {
    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} name - The new alert name
     * @return {undefined}
     */
    validateAlertName(name) {
      let isDuplicateName = false;

      const originalName = this.get('model.functionName');
      if (name === originalName) { return; }

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
     * Action handler for form submit
     * MVP Version: Can activate/deactivate and update alert name
     * sends a put request and refreshes model
     * @returns {Promise}
     */
    onSubmit() {
      const functionId = this.get('model.id');
      const functionName = this.get('alertFunctionName');
      const isActive = this.get('isActive');
      const url = `/dashboard/anomaly-function/${functionId}`;

      const body = {
        functionName,
        isActive
      };

      const putProps = {
        method: 'put',
        body: JSON.stringify(body),
        headers: { 'content-type': 'Application/Json' }
      };

      return fetch(url, putProps).then((res) => checkStatus(res, 'post'))
        .then(this.send('refreshModel'));
    }
  }
});
