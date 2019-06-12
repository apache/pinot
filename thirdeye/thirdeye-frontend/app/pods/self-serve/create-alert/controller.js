/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import { reads } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import fetch from 'fetch';
import Controller from '@ember/controller';
import { set, get } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import {toastOptions} from 'thirdeye-frontend/utils/constants';
import {
  selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import config from 'thirdeye-frontend/config/environment';

export default Controller.extend({
  notifications: service('toast'),

  /**
   * Initialized alert creation page settings
   */
  isAlertNameDuplicate: false,
  isAlertNameUserModified: false,
  helpDocLink: config.docs ? config.docs.createAlert : null,

  isForm: false,
  toggleCollapsed: true,              // flag for the accordion that hides/shows preview
  detectionYaml: null,                // The YAML for the anomaly detection
  subscriptionYaml:  null,            // The YAML for the subscription group
  alertDataIsCurrent: true,
  disableYamlSave: true,

  /**
   * Application name field options loaded from our model.
   */
  allApplicationNames: reads('model.applications'),

  /**
   * The debug flag
   */
  debug: reads('model.debug'),

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
   * Actions for create alert form view
   */
  actions: {
    changeAccordion() {
      set(this, 'toggleCollapsed', !get(this, 'toggleCollapsed'));
    },

    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} userProvidedName - The new alert name
     * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
     * If user-modified, we will stop modifying it dynamically (via 'isAlertNameUserModified')
     * @return {undefined}
     */
    validateAlertName(userProvidedName, userModified = false) {
      this._fetchAlertsByName(userProvidedName).then(matchingAlerts => {
        const isDuplicateName = matchingAlerts.find(alert => alert.functionName === userProvidedName);
        // If the user edits the alert name, we want to stop auto-generating it.
        if (userModified) {
          this.set('isAlertNameUserModified', true);
        }
        // Either add or clear the "is duplicate name" banner
        this.set('isAlertNameDuplicate', isDuplicateName);
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
     * update the subscription yaml string
     * @method updateSubscriptionYaml
     * @return {undefined}
     */
    updateSubscriptionYaml(updatedYaml) {
      set(this, 'subscriptionYaml', updatedYaml);
    },

    /**
     * update the subscription group object for dropdown
     * @method updateSubscriptionGroup
     * @return {undefined}
     */
    changeSubscriptionGroup(group) {
      this.setProperties({
        subscriptionYaml: group.yaml,
        groupName: group
      });
    },

    /**
     * Fired by create button in YAML UI
     * Grabs YAML content and sends it
     */
    createAlertYamlAction() {
      const content = {
        detection: get(this, 'detectionYaml'),
        subscription: get(this, 'subscriptionYaml')
      };
      const url = '/yaml/create-alert';
      const postProps = {
        method: 'post',
        body: JSON.stringify(content),
        headers: { 'content-type': 'application/json' }
      };
      const notifications = get(this, 'notifications');

      fetch(url, postProps).then((res) => {
        res.json().then((result) => {
          if(result){
            if (result.detectionMsg) {
              set(this, 'detectionMsg', result.detectionMsg);
            }
            if (result.subscriptionMsg) {
              set(this, 'subscriptionMsg', result.subscriptionMsg);
            }
            if (result.detectionAlertConfigId && result.detectionConfigId) {
              notifications.success('Created alert successfully.', 'Created', toastOptions);
            }
          }
        });
      }).catch((error) => {
        notifications.error('Create alert failed.', error, toastOptions);
      });
    }
  }
});
