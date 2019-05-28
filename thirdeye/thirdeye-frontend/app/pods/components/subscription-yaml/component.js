/**
 * Component to render the subscription group yaml editor.
 * @module components/subscription-yaml
 * @property {number} subscriptionGroupId - the subscription group id in edit mode
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {Object} subscriptionGroupNames - the list of subscription groups
 * @property {Object} subscriptionYaml - the subscription group yaml
 * @property {function} updateSubscriptionYaml - bubble up the subscription group yaml to parent
 * @example
   {{subscription-yaml
     subscriptionGroupId=model.subscriptionGroupId
     isEditMode=true
     subscriptionGroupNames=model.subscriptionGroupNames
     subscriptionYaml=model.subscriptionYaml
     setSubscriptionYaml=updateSubscriptionYaml
   }}
 * @authors lohuynh and hjackson
 */

import Component from '@ember/component';
import {computed, set, get, getProperties} from '@ember/object';
import {yamlAlertSettings, toastOptions} from 'thirdeye-frontend/utils/constants';
import fetch from 'fetch';
import {inject as service} from '@ember/service';
import {task} from 'ember-concurrency';
import config from 'thirdeye-frontend/config/environment';

const CREATE_GROUP_TEXT = 'Create a new subscription group';

export default Component.extend({
  classNames: ['yaml-editor'],
  notifications: service('toast'),
  /**
   * Properties we expect to receive for the yaml-editor
   */
  currentMetric: null,
  isYamlParseable: true,
  alertSettingsTitle: 'Define subscription configuration',
  isEditMode: false,
  showSettings: true,
  disableSubGroupSave: true,
  subscriptionMsg: '',                //General subscription failures
  subscriptionYaml:  null,            // The YAML for the subscription group
  currentYamlSettingsOriginal: yamlAlertSettings,
  showAnomalyModal: false,
  showNotificationModal: false,
  setSubscriptionYaml: null, // function passed in from parent



  init() {
    this._super(...arguments);
    const subscriptionGroupNamesDisplay = get(this, 'subscriptionGroupNamesDisplay');
    // Checks to make sure there is a subscription group array with at least one subscription group
    if (subscriptionGroupNamesDisplay && Array.isArray(subscriptionGroupNamesDisplay) && subscriptionGroupNamesDisplay.length > 0) {
      const firstGroup = subscriptionGroupNamesDisplay[0];
      const setSubscriptionYaml = get(this, 'setSubscriptionYaml');
      setSubscriptionYaml(firstGroup.yaml);
      set(this, 'groupName', firstGroup);
      set(this, 'subscriptionGroupId', firstGroup.id);
    }
  },

  /**
   * populates subscription group dropdown with options from fetch or model
   * @method subscriptionGroupNamesDisplay
   * @return {Object}
   */
  subscriptionGroupNamesDisplay: computed(
    'subscriptionGroupNames',
    async function() {
      const {
        isEditMode,
        subscriptionGroupNames
      } = this.getProperties('isEditMode', 'subscriptionGroupNames');
      const createGroup = {
        name: CREATE_GROUP_TEXT,
        id: 'n/a',
        yaml: yamlAlertSettings
      };
      const moddedArray = [createGroup];
      if (isEditMode) {
        return [...moddedArray, ...subscriptionGroupNames];
      }
      const subscriptionGroups = await get(this, '_fetchSubscriptionGroups').perform();
      return [...moddedArray, ...subscriptionGroups];
    }
  ),

  /**
   * Flag to trigger special case of no existing subscription groups for an alert
   * @method noExistingSubscriptionGroup
   * @return {Boolean}
   */
  noExistingSubscriptionGroup: computed(
    'subscriptionGroupNames',
    function() {
      const subscriptionGroupNames = get(this, 'subscriptionGroupNames');
      if (subscriptionGroupNames && Array.isArray(subscriptionGroupNames) && subscriptionGroupNames.length > 0) {
        return false;
      }
      return true;
    }
  ),

  /**
   * Change subscription group button text depending on whether creating or updating
   * @method subGroupButtonText
   * @return {String}
   */
  subGroupButtonText: computed(
    'noExistingSubscriptionGroup',
    'groupName',
    function() {
      const {
        noExistingSubscriptionGroup,
        groupName
      } = this.getProperties('noExistingSubscriptionGroup', 'groupName');
      return (noExistingSubscriptionGroup || !groupName || groupName.name === CREATE_GROUP_TEXT) ? "Create Group" : "Update Group";
    }
  ),

  /**
   * sets Yaml value displayed to contents of subscriptionYaml or currentYamlSettingsOriginal
   * @method currentSubscriptionYaml
   * @return {String}
   */
  currentSubscriptionYaml: computed(
    'subscriptionYaml',
    function() {
      const subscriptionYaml = get(this, 'subscriptionYaml');
      return subscriptionYaml || get(this, 'currentYamlSettingsOriginal');
    }
  ),

  isSubscriptionMsg: computed(
    'subscriptionMsg',
    function() {
      const subscriptionMsg = get(this, 'subscriptionMsg');
      return subscriptionMsg !== '';
    }
  ),

  _fetchSubscriptionGroups: task(function* () {
    //dropdown of subscription groups
    const url2 = `/detection/subscription-groups`;
    const postProps2 = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = get(this, 'notifications');

    try {
      const response = yield fetch(url2, postProps2);
      const json = yield response.json();
      return json.filterBy('yaml');
    } catch (error) {
      notifications.error('Failed to retrieve subscription groups.', 'Error', toastOptions);
    }
  }).drop(),

  // Method for handling subscription group, whether there are any or not
  async _handleSubscriptionGroup(subscriptionYaml, notifications, subscriptionGroupId) {
    const {
      noExistingSubscriptionGroup,
      groupName
    } = this.getProperties('noExistingSubscriptionGroup', 'groupName');
    if (noExistingSubscriptionGroup || !groupName || groupName.name === CREATE_GROUP_TEXT) {
      //POST settings
      const setting_url = '/yaml/subscription';
      const settingsPostProps = {
        method: 'POST',
        body: subscriptionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const settings_result = await fetch(setting_url, settingsPostProps);
        const settings_status  = get(settings_result, 'status');
        const settings_json = await settings_result.json();
        if (settings_status !== 200) {
          set(this, 'errorMsg', get(settings_json, 'message'));
          notifications.error(`Failed to save the subscription configuration due to: ${settings_json.message}.`, 'Error', toastOptions);
        } else {
          notifications.success('Subscription configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving subscription config.', error, toastOptions);
      }
    } else {
      //PUT settings
      const setting_url = `/yaml/subscription/${subscriptionGroupId}`;
      const settingsPostProps = {
        method: 'PUT',
        body: subscriptionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const settings_result = await fetch(setting_url, settingsPostProps);
        const settings_status  = get(settings_result, 'status');
        const settings_json = await settings_result.json();
        if (settings_status !== 200) {
          set(this, 'errorMsg', get(settings_json, 'message'));
          notifications.error(`Failed to save the subscription configuration due to: ${settings_json.message}.`, 'Error', toastOptions);
        } else {
          notifications.success('Subscription configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving subscription config.', error, toastOptions);
      }
    }
  },

  actions: {
    /**
     * resets given yaml field to default value for creation mode and server value for edit mode
     */
    resetYAML() {
      const setSubscriptionYaml = get(this, 'setSubscriptionYaml');
      setSubscriptionYaml(get(this, 'currentYamlSettingsOriginal'));
    },

    /**
     * Links to subscription group configuration section of wiki
     */
    triggerDoc() {
      window.open(config.docs.subscriptionConfig);
    },

    /**
     * Activates 'Create changes' button and stores YAML content in subscriptionYaml
     */
    onEditingSubscriptionYamlAction(value) {
      const setSubscriptionYaml = get(this, 'setSubscriptionYaml');
      setSubscriptionYaml(value);
      set(this, 'disableSubGroupSave', false);
    },

    /**
     * Updates the subscription settings yaml with user section
     */
    onSubscriptionGroupSelectionAction(value) {
      if(value.yaml) {
        const setSubscriptionYaml = get(this, 'setSubscriptionYaml');
        setSubscriptionYaml(value.yaml);
        set(this, 'groupName', value);
        set(this, 'subscriptionGroupId', value.id);
      }
    },

    /**
     * Fired by subscription group button in YAML UI in edit mode
     * Grabs subscription group yaml and posts or puts it to the backend.
     */
    async submitSubscriptionGroup() {
      const {
        subscriptionYaml,
        notifications,
        subscriptionGroupId
      } = getProperties(this, 'subscriptionYaml', 'notifications', 'subscriptionGroupId');
      // If there is no existing subscription group, this method will handle it
      this._handleSubscriptionGroup(subscriptionYaml, notifications, subscriptionGroupId);
    }
  }
});
