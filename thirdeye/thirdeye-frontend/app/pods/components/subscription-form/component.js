/**
 * Component to render the subscription configuration form editor.
 * @module components/subscription-form
 * @property {Array of Objects} allApplicationNames - applications from Ember data converted to vanilla objects
 * @property {function} validateAlertName - action passed in from parent, shared with detection-form component
 * @property {Array of Objects} allAlertsConfigGroups - subscription-groups from Ember Data converted to vanilla objects
 * @example
   {{subscription-form
     allApplicationNames=applicationNames
     validateAlertName=(action "parentAction")
     allAlertsConfigGroups=allAlertsConfigGroups
   }}
 * @author hjackson
 */

import Component from '@ember/component';
import {computed} from '@ember/object';
import {checkStatus} from 'thirdeye-frontend/utils/utils';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  isEmpty,
  isPresent
} from "@ember/utils";
import {
  selfServeApiGraph, selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import {
  formatConfigGroupProps
} from 'thirdeye-frontend/utils/manage-alert-utils';

export default Component.extend({
  alertFunctionName: null,
  /**
   * Properties we expect to receive for the subscription-form
   */
  isEditMode: false,
  generalFieldsEnabled: true,
  allApplicationNames: null,
  allAlertsConfigGroups: [],
  validateAlertName: null, // action passed in by parent


  /**
   * Generate alert name primer based on user selections
   * @type {String}
   */
  functionNamePrimer: computed(
    'selectedDimension',
    'selectedGranularity',
    'selectedApplication',
    'selectedMetricOption',
    function() {
      const {
        selectedDimension,
        selectedGranularity,
        selectedApplication,
        selectedMetricOption
      } = this.getProperties(
        'selectedDimension',
        'selectedGranularity',
        'selectedApplication',
        'selectedMetricOption'
      );
      const dimension = selectedDimension ? `${selectedDimension.camelize()}_` : '';
      const granularity = selectedGranularity ? selectedGranularity.toLowerCase().camelize() : '';
      const app = selectedApplication ? `${selectedApplication.camelize()}_` : 'applicationName_';
      const metric = selectedMetricOption ? `${selectedMetricOption.name.camelize()}_` : 'metricName_';
      return `${app}${metric}${dimension}${granularity}`;
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
      const activeGroups = this.get('allAlertsConfigGroups');
      const groupsWithAppName = activeGroups.filter(group => isPresent(group.application));

      if (isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName.toLowerCase()));
      } else {
        return activeGroups;
      }
    }
  ),

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method _fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  _fetchAlertsByName(functionName) {
    const url = selfServeApiCommon.alertFunctionByName(functionName);
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * Note: these requests can fail silently and any empty response will fall back on defaults.
   * @method _fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {RSVP.promise}
   */
  _fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(selfServeApiGraph.maxDataTime(metricId)).then(res => checkStatus(res, 'get', true)),
      granularities: fetch(selfServeApiGraph.metricGranularity(metricId)).then(res => checkStatus(res, 'get', true)),
      filters: fetch(selfServeApiGraph.metricFilters(metricId)).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(selfServeApiGraph.metricDimensions(metricId)).then(res => checkStatus(res, 'get', true))
    };
    return RSVP.hash(promiseHash);
  },

  /**
   * Auto-generate the alert name until the user directly edits it
   * @method _modifyAlertFunctionName
   * @return {undefined}
   */
  _modifyAlertFunctionName() {
    const {
      functionNamePrimer,
      isAlertNameUserModified
    } = this.getProperties('functionNamePrimer', 'isAlertNameUserModified');
    // If user has not yet edited the alert name, continue to auto-generate it.
    if (!isAlertNameUserModified) {
      this.set('alertFunctionName', functionNamePrimer);
    }
    // Each time we modify the name, we validate it as well to ensure no duplicates exist.
    this.get('validateAlertName')(this.get('alertFunctionName'));
  },

  /**
   * Enriches the list of functions by Id, adding the properties we may want to display.
   * We are preparing to display the alerts that belong to the currently selected config group.
   * TODO: Good candidate to move to self-serve shared services
   * @method _prepareFunctions
   * @param {Object} configGroup - the currently selected alert config group
   * @return {RSVP.Promise} A new list of functions (alerts)
   */
  _prepareFunctions(configGroup) {
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    const newFunctionList = [];
    let cnt = 0;

    // Build object for each function(alert) to display in results table
    return new RSVP.Promise((resolve) => {
      existingFunctionList.forEach((functionId) => {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push(formatConfigGroupProps(functionData));
          cnt ++;
          if (existingFunctionList.length === cnt) {
            resolve(newFunctionList);
          }
        });
      });
    });
  },

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
      this._modifyAlertFunctionName();
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
        selectedGroupToRecipients: null,
        selectedGroupCcRecipients: null,
        selectedGroupBccRecipients: null,
        isEmptyEmail: isEmpty(this.get('alertGroupNewRecipient'))
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
      let existingEmailArr = this.get('selectedGroupToRecipients');
      let cleanEmailArr = [];
      let badEmailArr = [];
      let isDuplicateEmail = false;

      // Release submit button error state
      this.setProperties({
        isEmailError: false,
        isProcessingForm: false,
        isEditedConfigGroup: true,
        isEmptyEmail: isPresent(this.get('newConfigGroupName')) && !emailInput.length
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
          duplicateEmails: badEmailArr.join(", ")
        });
      }
    },

    /**
     * Set our selected alert configuration group. If one is selected, display editable fields
     * for that group and display the list of functions that belong to that group.
     * @method onSelectConfigGroup
     * @param {Object} selectedObj - The selected config group option
     * @return {undefined}
     */
    onSelectConfigGroup(selectedObj) {
      const toAddr = ((selectedObj.receiverAddresses || []).to || []).join(", ");
      const ccAddr = ((selectedObj.receiverAddresses || []).cc || []).join(", ");
      const bccAddr = ((selectedObj.receiverAddresses || []).bcc || []).join(", ");
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        isEmptyEmail: isEmpty(toAddr),
        selectedGroupToRecipients: toAddr,
        selectedGroupCcRecipients: ccAddr,
        selectedGroupBccRecipients: bccAddr
      });
      this._prepareFunctions(selectedObj).then(functionData => {
        this.set('selectedGroupFunctions', functionData);
      });
    }
  }
});
