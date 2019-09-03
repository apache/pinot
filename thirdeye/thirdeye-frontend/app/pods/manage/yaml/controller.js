/**
 * Controller for Edit Alert page
 * @module manage/yaml/{alertId}
 * @exports manage/yaml/{alertId}
 */
import Controller from '@ember/controller';
import {computed, set, get, getProperties} from '@ember/object';
import {toastOptions} from 'thirdeye-frontend/utils/constants';
import {inject as service} from '@ember/service';

const CREATE_GROUP_TEXT = 'Create a new subscription group';

export default Controller.extend({
  notifications: service('toast'),
  alertDataIsCurrent: true,
  disableYamlSave: true,
  toggleCollapsed: true,              // flag for the accordion that hides/shows preview
  disableSubGroupSave: true,

  /**
   * Change subscription group button text depending on whether creating or updating
   * @method subGroupButtonText
   * @return {String}
   */
  subGroupButtonText: computed(
    'groupName',
    function() {
      const groupName = get(this, 'groupName');
      return (!groupName || groupName.name === CREATE_GROUP_TEXT) ? "Create Group" : "Update Group";
    }
  ),

  // Method for handling subscription group, whether there are any or not
  async _handleSubscriptionGroup(subscriptionYaml, notifications, subscriptionGroupId) {
    const groupName = get(this, 'groupName');
    if (!groupName || groupName.name === CREATE_GROUP_TEXT) {
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
    changeAccordion() {
      set(this, 'toggleCollapsed', !get(this, 'toggleCollapsed'));
    },

    /**
     * update the detection yaml string
     * @method updateDetectionYaml
     * @return {undefined}
     */
    updateDetectionYaml(updatedYaml) {
      this.setProperties({
        detectionYaml: updatedYaml,
        alertDataIsCurrent: false,
        disableYamlSave: false
      });
    },

    /**
     * update the subscription yaml string  and activates 'create group' button
     * @method updateSubscriptionYaml
     * @return {undefined}
     */
    updateSubscriptionYaml(updatedYaml) {
      this.setProperties({
        disableSubGroupSave: false,
        subscriptionYaml: updatedYaml
      });
    },

    /**
     * update the subscription group object for dropdown
     * @method updateSubscriptionGroup
     * @return {undefined}
     */
    changeSubscriptionGroup(group) {
      this.setProperties({
        subscriptionYaml: group.yaml,
        groupName: group,
        subscriptionGroupId: group.id
      });
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
