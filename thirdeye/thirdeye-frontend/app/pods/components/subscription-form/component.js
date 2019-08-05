/**
 * Component to render the subscription configuration form editor.
 * @module components/subscription-form
 * @property {String} subscriptionYaml - yaml configuration of subscription group
 * @property {boolean} isEditMode - flag to control submit button and method used for submiting changes
 * @property {Array of Objects} allApplicationNames - applications from Ember data converted to vanilla objects
 * @property {Array of Objects} allSubscriptionGroups - subscription-groups from Ember Data converted to vanilla objects
 * @property {boolean} generalFieldsEnabled - flag to disable boxes for input flow control
 * @property {function} selectSubscriptionGroup - keeps track of selected group so subscription-form and subscription-yaml stay in sync
 * @property {function} setSubscriptionYaml - updates yaml to keep subscription-form and subscription-yaml in sync
 * @example
   {{subscription-form
     isEditMode=false
     subscriptionYaml=subscriptionYaml
     allApplicationNames=applicationNames
     allSubscriptionGroups=allSubscriptionGroups
     generalFieldsEnabled=generalFieldsEnabled
     selectSubscriptionGroup=(action "changeSubscriptionGroup")
     setSubscriptionYaml=(action "updateSubscriptionYaml")
   }}
 * @author hjackson
 */

import Component from '@ember/component';
import { computed, get, set } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { defaultSubscriptionYaml, fieldsToYaml, redundantParse } from 'thirdeye-frontend/utils/yaml-tools';
import fetch from 'fetch';
import {
  isEmpty,
  isPresent
} from "@ember/utils";
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';

export default Component.extend({
  alertFunctionName: null,
  generalFieldsEnabled: false, // controlled by parent, do not set
  /**
   * Properties we expect to receive for the subscription-form
   */
  isEditMode: false,
  subscriptionYaml: null,
  allApplicationNames: null,
  allSubscriptionGroups: [],
  newSubGroupName: null,
  selectedSubGroup: {},
  selectSubscriptionGroup: null, // controlled by parent, do not set
  setSubscriptionYaml: null, // controlled by parent, do not set
  subscriptionGroupNewRecipient: null,


  /**
   * Filter all existing alert groups down to only those that are active and belong to the
   * currently selected application team.
   * @method filteredSubGroups
   * @param {String} selectedAppName - user-selected application name
   * @return {Array} activeGroups - filtered list of groups that are active
   */
  filteredSubGroups: computed(
    'selectedApplication',
    function() {
      const appName = this.get('selectedAppName');
      const activeGroups = this.get('allSubscriptionGroups');
      const groupsWithAppName = activeGroups.filter(group => isPresent(group.application));

      if (isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName.toLowerCase()));
      } else {
        return activeGroups;
      }
    }
  ),

  updatedFields: computed(
    'selectedAppName',
    'newSubGroupName',
    'groupName',
    'subscriptionGroupNewRecipient',
    function() {
      const {
        selectedAppName,
        newSubGroupName,
        groupName,
        subscriptionGroupNewRecipient
      } = this.getProperties('selectedAppName', 'newSubGroupName', 'groupName', 'subscriptionGroupNewRecipient');
      return (selectedAppName || newSubGroupName || groupName || subscriptionGroupNewRecipient);
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

  init() {
    this._super(...arguments);
    const subscriptionYaml = get(this, 'subscriptionYaml');
    if (subscriptionYaml && subscriptionYaml !== defaultSubscriptionYaml) {
      this._getFieldsFromYaml(subscriptionYaml);
    }
  },

  /**
   * This hook will integrate form changes with yaml and send the new yaml to parent
   * so that the changes are reflected in detection-yaml
   * @method willDestroyElement
   * @return (undefined)
   */
  willDestroyElement() {
    const {
      newSubGroupName,
      selectedSubGroup,
      selectedGroupToRecipients,
      subscriptionGroupNewRecipient
    } = this.getProperties('newSubGroupName', 'selectedSubGroup', 'selectedGroupToRecipients', 'subscriptionGroupNewRecipient');
    if (get(this, 'updatedFields')){
      let subscriptionYaml = get(this, 'subscriptionYaml');
      const updatedFields = {};
      if (newSubGroupName) {
        updatedFields.application = this.get('selectedAppName');
        updatedFields.subscriptionGroupName = (newSubGroupName || selectedSubGroup.name);
        // to-do: put rules in here after rule component is operational
      } else if (selectedSubGroup.yaml){
        // if the user is editing an existing group, update group's yaml
        subscriptionYaml = selectedSubGroup.yaml;
      }
      let to = selectedGroupToRecipients ? selectedGroupToRecipients.split(', ') : [];
      if (subscriptionGroupNewRecipient)
      {
        to = [...to, ...subscriptionGroupNewRecipient.replace(/\s/g, '').split(',')];
      }
      if (to.length > 0) {
        const recipients = {
          to
        };
        updatedFields.recipients = recipients;
      }
      // send fields and yaml to yaml util to update values
      subscriptionYaml = fieldsToYaml(updatedFields, subscriptionYaml, defaultSubscriptionYaml);
      // send new Yaml to parent
      this.get('setSubscriptionYaml')(subscriptionYaml);
    }
  },

  /**
   * If the yaml is passed in, then parse the relevant fields to props for the form
   * Assumes yaml string is not the default subscription yaml
   * @method _getFieldsFromYaml
   * @param {String} yaml - current yaml string
   * @return {undefined}
   */
  _getFieldsFromYaml(yaml) {
    let yamlAsObject = {};
    let dy = {}; // dy = default yaml
    let appAsObject, appApplication, subscriptionGroup;
    try {
      yamlAsObject = redundantParse(yaml);
      dy = redundantParse(defaultSubscriptionYaml);
      set(this, 'isYamlParseable', true);
    } catch(err) {
      set(this, 'isYamlParseable', false);
      return null;
    }
    // if application is selected, we can build selectedApplication and populate the dropdown
    appApplication = yamlAsObject.application;
    if (appApplication && appApplication !== dy.application) {
      appAsObject = {
        application: appApplication
      };
    }
    const isSubGroupName = (yamlAsObject.subscriptionGroupName && yamlAsObject.subscriptionGroupName !== dy.subscriptionGroupName);
    // if there is a subscription group named, let's try to match it to an existing one
    if (isSubGroupName) {
      subscriptionGroup = get(this, 'allSubscriptionGroups')
        .find(group => group.name === yamlAsObject.subscriptionGroupName);
      if (subscriptionGroup){
        // change yaml to user-modified version for further changes
        subscriptionGroup.yaml = yaml;
        // send to action to populate email field
        this.send('onSelectSubGroup', subscriptionGroup);
      } else {
        // if no subgroup match but the name is there, must be a new name
        subscriptionGroup = {
          name: yamlAsObject.subscriptionGroupName,
          yaml
        };
        // send to action to populate email field
        this.send('onSelectSubGroup', subscriptionGroup);
      }
    }
    // we only use fields that have been updated
    this.setProperties({
      selectedApplication: appAsObject
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
        selectedApplication: selectedObj,
        selectedAppName: selectedObj.application
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
      const nameExists = this.get('allSubscriptionGroups')
        .map(group => group.name)
        .includes(name);

      // set error message and return early if group name exists
      if (nameExists) {
        this.set('isGroupNameDuplicate', true);
        return;
      }

      this.setProperties({
        newSubGroupName: name,
        selectedSubGroup: null,
        selectedGroupToRecipients: null,
        selectedGroupCcRecipients: null,
        selectedGroupBccRecipients: null,
        isEmptyEmail: isEmpty(this.get('subscriptionGroupNewRecipient'))
      });
    },

    /**
     * Verify that email address does not already exist in alert group. If it does, remove it and alert user.
     * @method validateAlertEmail
     * @param {String} emailInput - Comma-separated list of new emails to add to the Sub group.
     * @return {undefined}
     */
    validateAlertEmail(emailInput) {
      const newEmailArr = emailInput.replace(/\s+/g, '').split(',');
      const existingEmailStr = this.get('selectedGroupToRecipients');
      let badEmailArr = [];
      let isDuplicateEmail = false;

      // Release submit button error state
      this.setProperties({
        isEmailError: false,
        isProcessingForm: false,
        isEditedSubGroup: true,
        isEmptyEmail: isPresent(this.get('newSubGroupName')) && !emailInput.length
      });

      // Check for duplicates
      if (emailInput.trim() && existingEmailStr) {
        const existingEmailArr = existingEmailStr.replace(/\s+/g, '').split(',');
        newEmailArr.forEach(email => {
          if (email.length && existingEmailArr.includes(email)) {
            isDuplicateEmail = true;
            badEmailArr.push(email);
          }
        });
        this.setProperties({
          isDuplicateEmail,
          duplicateEmails: badEmailArr.join(", ")
        });
      }
    },

    /**
     * Set our selected subscription group. If one is selected, display editable fields
     * for that group and display the list of functions that belong to that group.
     * @method onSelectSubGroup
     * @param {Object} selectedObj - The selected Sub group option
     * @return {undefined}
     */
    onSelectSubGroup(selectedObj) {
      let groupConfig = {};
      if (selectedObj.yaml) {
        this.get('selectSubscriptionGroup')(selectedObj);
        try {
          groupConfig = redundantParse(selectedObj.yaml);
          set(this, 'isYamlParseable', true);
        } catch(err) {
          set(this, 'isYamlParseable', false);
        }
      }
      const toAddr = ((groupConfig.recipients || []).to || []).join(", ");
      const ccAddr = ((groupConfig.recipients || []).cc || []).join(", ");
      const bccAddr = ((groupConfig.recipients || []).bcc || []).join(", ");
      this.setProperties({
        selectedSubGroup: selectedObj,
        newSubGroupName: null,
        isEmptyEmail: isEmpty(toAddr),
        selectedGroupToRecipients: toAddr,
        selectedGroupCcRecipients: ccAddr,
        selectedGroupBccRecipients: bccAddr
      });
    }
  }
});
