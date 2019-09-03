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
     isEditMode=true
     subscriptionYaml=model.subscriptionYaml
     setSubscriptionYaml=(action "updateSubscriptionYaml")
     subscriptionMsg={string} //Optional error message to surface
     selectSubscriptionGroup=(action "changeSubscriptionGroup")
     subscriptionGroupNamesDisplay=subscriptionGroupNamesDisplay
     groupName=groupName
     createGroup=createGroup // default group for reset
   }}
 * @authors lohuynh and hjackson
 */

import Component from '@ember/component';
import {computed, get, set} from '@ember/object';
import {yamlAlertSettings} from 'thirdeye-frontend/utils/constants';
import {inject as service} from '@ember/service';
import config from 'thirdeye-frontend/config/environment';

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
  createGroup: null,



  isSubscriptionMsg: computed(
    'subscriptionMsg',
    function() {
      const subscriptionMsg = get(this, 'subscriptionMsg');
      return subscriptionMsg !== '';
    }
  ),

  init() {
    this._super(...arguments);
    const {
      subscriptionYaml,
      currentYamlSettingsOriginal
    } = this.getProperties('subscriptionYaml', 'currentYamlSettingsOriginal');
    if (!subscriptionYaml) {
      set(this, 'subscriptionYaml', currentYamlSettingsOriginal);
    }
  },

  actions: {
    /**
     * resets given yaml field to default value for creation mode and server value for edit mode
     */
    resetYAML() {
      const {
        selectSubscriptionGroup,
        createGroup,
        subscriptionGroupNamesDisplay,
        isEditMode
      } = this.getProperties('selectSubscriptionGroup', 'createGroup', 'subscriptionGroupNamesDisplay', 'isEditMode');
      isEditMode ? selectSubscriptionGroup(createGroup) : selectSubscriptionGroup(subscriptionGroupNamesDisplay[0]);
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
    },

    /**
     * Updates the subscription settings yaml with user section
     */
    onSubscriptionGroupSelectionAction(value) {
      if(value.yaml) {
        const selectSubscriptionGroup = get(this, 'selectSubscriptionGroup');
        selectSubscriptionGroup(value);
      }
    }
  }
});
