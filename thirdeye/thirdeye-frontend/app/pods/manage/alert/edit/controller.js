/**
 * Handles alert edit form
 * @module manage/alert/edit
 * @exports manage/alert/edit
 */
import { reads, or } from '@ember/object/computed';
import fetch from 'fetch';
import Controller from '@ember/controller';
import {
  set,
  get,
  computed,
  setProperties
} from '@ember/object';
import {
  checkStatus,
  postProps
} from 'thirdeye-frontend/utils/utils';
import {
  selfServeApiCommon,
  selfServeApiOnboard
} from 'thirdeye-frontend/utils/api/self-serve';

export default Controller.extend({

  /**
   * Optional query param to refresh model
   */
  queryParams: ['refresh'],
  refresh: null,

  /**
   * Important initializations
   */
  isEditAlertSuccess: false, // alert save success
  isProcessingForm: false, // to trigger submit disable
  isExiting: false, // exit detection
  showManageGroupsModal: false, // manage group modal

  /**
   * The config group that the current alert belongs to
   * @type {Object}
   */
  originalConfigGroup: reads('model.originalConfigGroup'),

  /**
   * Mapping alertFilter's pattern to human readable strings
   * @returns {String}
   */
  pattern: computed('alertProps', function() {
    const props = this.get('alertProps');
    const patternObj = props.find(prop => prop.name === 'pattern');
    const pattern = patternObj ? decodeURIComponent(patternObj.value) : 'Up and Down';

    return pattern;
  }),

  /**
   * Extracting Weekly Effect from alert Filter
   * @returns {String}
   */
  weeklyEffect: computed('alertFilters.weeklyEffectModeled', function() {
    const weeklyEffect = this.getWithDefault('alertFilters.weeklyEffectModeled', true);

    return weeklyEffect;
  }),

  /**
   * Extracting sensitivity from alert Filter and maps it to human readable values
   * @returns {String}
   */
  sensitivity: computed('alertProps', function() {
    const props = this.get('alertProps');
    const sensitivityObj = props.find(prop => prop.name === 'sensitivity');
    const sensitivity = sensitivityObj ? decodeURIComponent(sensitivityObj.value) : 'MEDIUM';

    const sensitivityMapping = {
      LOW: 'Robust (Low)',
      MEDIUM: 'Medium',
      HIGH: 'Sensitive (High)'
    };

    return sensitivityMapping[sensitivity];
  }),

  /**
   * Disable submit under these circumstances
   * @method isSubmitDisabled
   * @return {Boolean} show/hide submit
   */
  isSubmitDisabled: or('{isProcessingForm,isAlertNameDuplicate}'),

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method _fetchAlertByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  _fetchAlertByName(functionName) {
    const url = selfServeApiCommon.alertFunctionByName(functionName);
    return fetch(url).then(checkStatus);
  },

  /**
   * Display success banners while model reloads
   * @method confirmEditSuccess
   * @return {undefined}
   */
  confirmEditSuccess() {
    this.set('isEditAlertSuccess', true);
  },

  /**
   * Reset fields to model init state
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      model: null,
      isExiting: true,
      isSubmitDisabled: false,
      isEmailError: false,
      isDuplicateEmail: false,
      isEditAlertSuccess: false,
      isNewConfigGroupSaved: false,
      isProcessingForm: false,
      isActive: false,
      isLoadError: false,
      updatedRecipients: [],
      granularity: null,
      alertFilters: null,
      alertFunctionName: null,
      alertId: null,
      loadErrorMessage: null
    });
  },

  /**
   * Actions for edit alert form view
   */
  actions: {

    /**
     * Make sure alert name does not already exist in the system
     * Either add or clear the "is duplicate name" banner
     * @method validateAlertName
     * @param {String} userProvidedName - The new alert name
     * @return {undefined}
     */
    validateAlertName(userProvidedName) {
      this._fetchAlertByName(userProvidedName).then(matchingAlerts => {
        const isDuplicateName = matchingAlerts.find(alert => alert.functionName === userProvidedName);
        this.set('isAlertNameDuplicate', isDuplicateName);
      });
    },

    /**
     * Action handler for displaying groups modal
     * @returns {undefined}
     */
    onShowManageGroupsModal() {
      set(this, 'showManageGroupsModal', true);
    },

    /**
     * Action handler for CANCEL button - simply reset all fields
     * @returns {undefined}
     */
    onCancel() {
      const alertId = get(this, 'alertId');
      this.send('refreshModel');
      this.transitionToRoute('manage.alert.explore', alertId);
    },

    /**
     * Action handler for form submit
     * MVP Version: Can activate/deactivate and update alert name and edit config group data
     * @returns {Promise}
     */
    onSubmit() {
      const {
        isActive,
        alertFunctionName,
        alertData: postFunctionBody
      } = this.getProperties(
        'isActive',
        'alertFunctionName',
        'alertData'
      );

      // Disable submit for now and make sure we're clear of email errors
      set(this, 'isProcessingForm', true);

      // Assign these fresh editable values to the Alert object currently being edited
      setProperties(postFunctionBody, {
        isActive,
        functionName: alertFunctionName
      });

      // Step 1: Save any edits to the Alert entity in our DB
      return fetch(selfServeApiOnboard.editAlert, postProps(postFunctionBody))
        .then(res => checkStatus(res, 'post'))
        .then(() => {
          this.send('confirmSaveStatus', true);
          set(this, 'isProcessingForm', false);
        })
        .catch(() => {
          this.send('confirmSaveStatus', false);
          this.clearAll();
        });
    }
  }
});
