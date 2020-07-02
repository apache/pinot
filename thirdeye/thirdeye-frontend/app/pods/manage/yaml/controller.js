/**
 * Controller for Edit Alert page
 * @module manage/yaml/{alertId}
 * @exports manage/yaml/{alertId}
 */
import Controller from '@ember/controller';
import {computed, set, get, getProperties} from '@ember/object';
import {toastOptions} from 'thirdeye-frontend/utils/constants';
import {inject as service} from '@ember/service';
import { putAlertActiveStatus } from 'thirdeye-frontend/utils/anomaly';

const CREATE_GROUP_TEXT = 'Create a new subscription group';

export default Controller.extend({
  notifications: service('toast'),
  alertDataIsCurrent: true,
  disableYamlSave: true,
  toggleCollapsed: true,              // flag for the accordion that hides/shows preview
  disableSubGroupSave: true,
  subscriptionError: false,
  subscriptionErrorMsg: null,
  subscriptionErrorInfo: null,
  previewError: false,
  previewErrorMsg: null,
  previewErrorInfo: null,

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
    set(this, 'subscriptionError', false);
    const groupName = get(this, 'groupName');
    if (!groupName || groupName.name === CREATE_GROUP_TEXT) {
      //POST subscription
      const subscription_url = '/yaml/subscription';
      const subscriptionPostProps = {
        method: 'POST',
        body: subscriptionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const subscription_result = await fetch(subscription_url, subscriptionPostProps);
        const subscription_status  = get(subscription_result, 'status');
        const subscription_json = await subscription_result.json();
        if (subscription_status !== 200) {
          set(this, 'errorMsg', get(subscription_json, 'message'));
          notifications.error(`Failed to save the subscription configuration due to: ${subscription_json.message}.`, 'Error', toastOptions);
          this.setProperties({
            subscriptionError: true,
            subscriptionErrorMsg: subscription_json.message,
            subscriptionErrorInfo: subscription_json["more-info"]
          });
        } else {
          notifications.success('Subscription configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving subscription config.', error, toastOptions);
        this.setProperties({
          subscriptionError: true,
          subscriptionErrorMsg: 'Error while saving subscription config.',
          subscriptionErrorInfo: error
        });
      }
    } else {
      //PUT subscription
      const subscription_url = `/yaml/subscription/${subscriptionGroupId}`;
      const subscriptionPostProps = {
        method: 'PUT',
        body: subscriptionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const subscription_result = await fetch(subscription_url, subscriptionPostProps);
        const subscription_status  = get(subscription_result, 'status');
        const subscription_json = await subscription_result.json();
        if (subscription_status !== 200) {
          set(this, 'errorMsg', get(subscription_json, 'message'));
          notifications.error(`Failed to save the subscription configuration due to: ${subscription_json.message}.`, 'Error', toastOptions);
          this.setProperties({
            subscriptionError: true,
            subscriptionErrorMsg: subscription_json.message,
            subscriptionErrorInfo: subscription_json["more-info"]
          });
        } else {
          notifications.success('Subscription configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving subscription config.', error, toastOptions);
        this.setProperties({
          subscriptionError: true,
          subscriptionErrorMsg: 'Error while saving subscription config.',
          subscriptionErrorInfo: error
        });
      }
    }
  },

  actions: {
    changeAccordion() {
      set(this, 'toggleCollapsed', !get(this, 'toggleCollapsed'));
    },

    /**
     * toggle the active status of alert being displayed
     * @method toggleActivation
     * @return {undefined}
     */
    toggleActivation() {
      const detectionConfigId = this.get('model.alertId');
      putAlertActiveStatus(detectionConfigId, !this.get('model.alertData.isActive'))
        .then(() => this.send('refreshModel'))
        .catch(error => {
          this.get('notifications')
            .error(`Failed to set active flag of detection config ${detectionConfigId}: ${(typeof error === 'object' ? error.message : error)}`,
              'Error',
              toastOptions);
        });
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
     * set preview error for pushing down to detection-yaml component
     * @method setPreviewError
     * @return {undefined}
     */
    setPreviewError(bubbledObject) {
      this.setProperties({
        previewError: bubbledObject.previewError,
        previewErrorMsg: bubbledObject.previewErrorMsg,
        previewErrorInfo: bubbledObject.previewErrorInfo
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
