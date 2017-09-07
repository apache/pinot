/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import fetch from 'fetch';
import Ember from 'ember';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Controller.extend({

  metricName: Ember.computed.reads('model.metric'),
  granularity: Ember.computed.reads('model.bucketUnit'),
  dimensions: Ember.computed.reads('model.exploreDimensions'),
  alertFilter: Ember.computed.reads('model.alertFilter'),
  alertFunctionName: Ember.computed.reads('model.functionName'),
  isActive: Ember.computed.reads('model.isActive'),
  selectedAppName: Ember.computed.reads('model.application'),
  metricData: Ember.computed.reads('model.metricData'),
  subscriptionGroup: Ember.computed.reads('model.subscriptionGroup'),

  filters: Ember.computed('model.filters', function() {
    return this.get('model.filters');
  }),

  pattern: Ember.computed('alertFilter.pattern', function() {
    const pattern = this.getWithDefault('alertFilter.pattern', 'UP,DOWN');

    const patternMapping = {
      'UP,DOWN': 'Up and Down',
      UP: 'Up',
      DOWN: 'Down'
    };

    return patternMapping[pattern];
  }),

  weeklyEffect: Ember.computed('alertFilter.weeklyEffectModeled', function() {
    const weeklyEffect = this.getWithDefault('alertFilter.weeklyEffectModeled', true);

    return weeklyEffect;
  }),

  sensitivity: Ember.computed('alertFilter.userDefinedPattern', function() {
    const sensitivity = this.getWithDefault('alertFilter.userDefinedPattern', 'MEDIUM');
    const sensitivityMapping = {
      LOW: 'Robust',
      MEDIUM: 'Medium',
      HIGHT: 'Sensitive'
    };

    return sensitivityMapping[sensitivity];
  }),

  selectedApplication: Ember.computed.reads('model.subscriptionGroup.application'),

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
   * Filter all existing alert groups down to only those that are active and belong to the
   * currently selected application team.
   * @method filteredConfigGroups
   * @param {Object} selectedApplication - user-selected application object
   * @return {Array} activeGroups - filtered list of groups that are active
   */
  filteredConfigGroups: Ember.computed(
    'selectedApplication',
    function() {

      return this.get('model.subscriptionGroup.name');
      // const appName = this.get('selectedApplication');
      // const activeGroups = this.getWithDefault('allAlertsConfigGroups', []).filterBy('active');
      // const groupsWithAppName = activeGroups.filter(group => Ember.isPresent(group.application));

      // if (Ember.isPresent(appName)) {
      //   return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName));
      // } else {
      //   return activeGroups;
      // }
    }
  ),

  /**
   * Actions for edit alert form view
   */
  actions: {
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
      // this.prepareFunctions(selectedObj).then(functionData => {
      //   this.set('selectedGroupFunctions', functionData);
      // });
    },

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

    // MVP version:
    // Can activate / deactivate
    // can change name of alerts
    onSubmit() {
      const functionId = this.get('model.id');
      const functionName = this.get('alertFunctionName');
      const isActive = this.get('isActive');
      const url = `/dashboard/anomaly-function/${functionId}`;

      const body = {
        functionName,
        isActive
      };

      debugger;

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
