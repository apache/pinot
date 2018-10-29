/**
 * Handles metrics import from inGrahps dashboards
 * @module self-serve/create/import-metric
 * @exports import-metric
 */
import fetch from 'fetch';
import Controller from '@ember/controller';
import { and } from '@ember/object/computed';
import { isPresent } from '@ember/utils';
import { computed } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { selfServeApiOnboard } from 'thirdeye-frontend/utils/api/self-serve';

export default Controller.extend({
  /**
   * Import Defaults
   */
  isImportSuccess: false,
  isImportError: false,
  isSubmitDone: false,
  isDashboardExistError: false,
  isCustomDashFieldDisabled: false,
  consolidateOptions: ['Aggregate', 'Average'],
  selectedConsolidateOption: 'Average',
  failureMessage: 'Metrics not onboarded. Please check your dashboard name or RRD.',
  importExistingDashboardFabricGroup: 'prod',

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @return {Boolean} isDisabled
   */
  isSubmitDisabled: computed(
    'importExistingDashboardName',
    'importCustomNewDataset',
    'importCustomNewMetric',
    'importCustomNewRrd',
    'isDashboardExistError',
    function() {
      let isDisabled = true;
      const existingNameField = this.get('importExistingDashboardName');
      const isExistingNameError = this.get('isDashboardExistError');
      const newNameField = this.get('importCustomNewDataset');
      const newMetricField = this.get('importCustomNewMetric');
      const newRrdField = this.get('importCustomNewRrd');
      // If existing dashboard field is filled, release submit button.
      if (isPresent(existingNameField) && !isExistingNameError) {
        isDisabled = false;
      }
      // If any of the 'import custom' fields are filled, assume user will go the RRD import route. Disable submit.
      if (isPresent(newNameField) || isPresent(newRrdField) || isPresent(newMetricField)) {
        isDisabled = true;
        // Enable submit if all required RRD fields are present
        if (isPresent(newNameField) && isPresent(newRrdField) && isPresent(newMetricField)) {
          isDisabled = false;
        }
      }
      return isDisabled;
    }
  ),

  /**
   * Determines whether or not the existing dashboard import mode is active or not.
   * @method isExistingDashboardNameFieldDisabled
   * @return {Boolean} isExistingDashFieldDisabled
   */
  isExistingDashFieldDisabled: computed(
    'importExistingDashboardName',
    'importCustomNewDataset',
    'importCustomNewMetric',
    'importCustomNewRrd',
    'isSubmitDone',
    function() {
      const rrd = this.get('importCustomNewRrd');
      const name = this.get('importCustomNewDataset');
      const metric = this.get('importCustomNewMetric');
      const isSubmitted = this.get('isSubmitDone');
      return isPresent(rrd) || isPresent(name) || isPresent(metric) || isSubmitted;
    }
  ),

  /**
   * Determines whether all fields are disabled (after submit)
   * @method isFormDisabled
   * @return {Boolean} isFormDisabled
   */
  isFormDisabled: and('isExistingDashFieldDisabled', 'isCustomDashFieldDisabled'),

  /**
   * Validates whether the entered dashboard name exists in inGraphs
   * @method validateDashboardName
   * @param {Boolean} isRrdImport - indicates whether we are importing custom dashboard name
   * @param {String} dashboardName - name of dashboard to import
   * @return {Promise}
   */
  validateDashboardName(isRrdImport, dashboardName, fabricGroup) {
    const url = selfServeApiOnboard.dashboardByName(dashboardName, fabricGroup);
    // Only make the call if we are importing an existing inGraphs dashboard (isRrdImport = false)
    if (isRrdImport) {
      return Promise.resolve(true);
    } else {
      return fetch(url).then(checkStatus);
    }
  },

  /**
   * Fetches a list of existing metrics by dataset name
   * @method fetchMetricsList
   * @param {String} dataSet - name of dataset to use in lookup
   * @return {Promise}
   */
  fetchMetricsList(dataSet) {
    const url = selfServeApiOnboard.metricsByDataset(dataSet);
    return fetch(url).then(checkStatus);
  },

  /**
   * Triggers instant onboard for the generated metrics. If not triggered manually here,
   * the onboard service runs every 15 minutes anyway.
   * @method triggerInstantOnboard
   * @return {Ember.RSVP.Promise}
   */
  triggerInstantOnboard() {
    const url = selfServeApiOnboard.triggerInstantOnboard;
    return fetch(url, { method: 'post' }).then((res) => checkStatus(res, 'post'));
  },

  /**
   * Generates/updates the metrics for an existing inGraphs dashboard
   * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Onboarding+ingraph+dashboards
   * @method onboardNewDataset
   * @param {Object} importObj - Request data containing dataset or metric name
   * @return {Ember.RSVP.Promise}
   */
  onboardNewDataset(importObj) {
    const postProps = {
      method: 'post',
      body: JSON.stringify(importObj),
      headers: { 'content-type': 'Application/Json' }
    };
    const url = selfServeApiOnboard.createNewDataset;
    return fetch(url, postProps).then((res) => checkStatus(res, 'post'));
  },

  /**
   * Sets the error message for any failed call and throws the error
   * @method setErrorState
   * @param {String} error - the error statusText set by our checkStatus helper
   * @param {String} message - the appropriate error message
   * @return {undefined}
   */
  setErrorState(error, message) {
    this.setProperties({
      isImportError: true,
      failureMessage: `${message}. (${error})`
    });
    throw error;
  },

  /**
   * Generates/updates the metrics for an existing inGraphs dashboard
   * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Onboarding+ingraph+dashboards
   * @method processNewImportSequence
   * @param {Boolean} isRrdImport - Indicates mode of import request
   * @param {Object} importObj - Request data containing dataset or metric name
   * @return {Ember.RSVP.Promise}
   */
  processNewImportSequence(isRrdImport, importObj) {
    // Make request to create new dataset name in TE Database
    this.onboardNewDataset(importObj)
      // Make request to trigger instant onboard
      .then((importResult) => {
        return this.triggerInstantOnboard();
      })
      .catch((error) => {
        this.setErrorState(error, 'Failed to create dataset in Third Eye DB');
      })
      // Check for metrics in TE that belong to the new dataset name
      .then((onboardResult) => {
        return this.fetchMetricsList(importObj.datasetName);
      })
      .catch((error) => {
        this.setErrorState(error, 'Failed to trigger metric onboard in Third Eye');
      })
      // If this is a custom dashboard import, and no metrics returned, assume something went wrong.
      .then((metricsList) => {
        if (isRrdImport && !metricsList.Records.length) {
          throw new Error('No metrics onboarded');
        } else {
          this.setProperties({
            isImportSuccess: true,
            importedMetrics: metricsList.Records
          });
        }
      })
      .catch((error) => {
        this.setErrorState(error, 'Failed to list newly onboarded metrics');
      });
  },

  /**
   * Defined actions for component
   */
  actions: {

    /**
     * Reset the form... clear all important fields
     * @method clearAll
     * @return {undefined}
     */
    clearAll() {
      this.setProperties({
        isCustomDashFieldDisabled: false,
        isImportSuccess: false,
        isImportError: false,
        isDashboardExistError: false,
        isSubmitDone: false,
        importExistingDashboardName: '',
        importExistingDashboardFabricGroup: 'prod',
        importCustomNewDataset: '',
        importCustomNewMetric: '',
        importCustomNewRrd: '',
        importCustomConsolidate: 'Average',
        datasetName: ''
      });
    },

    /**
     * Clears the validation error as user begins to type in field
     * @method clearExistingDashboardNameError
     * @return {undefined}
     */
    clearExistingDashboardNameError() {
      if (this.get('isDashboardExistError')) {
        this.set('isDashboardExistError', false);
      }
    },

    /**
     * Handles form submit
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      const isRrdImport = this.get('isExistingDashFieldDisabled');
      const datasetName = isRrdImport ? this.get('importCustomNewDataset') : this.get('importExistingDashboardName');
      const fabricGroup = this.get('importExistingDashboardFabricGroup') || 'prod';

      const importObj = {
        datasetName,
        dataSource: 'AutometricsThirdeyeDataSource',
        properties: {
          FABRIC_GROUP: fabricGroup
        }
      };

      // Enhance request payload for custom metrics
      if (isRrdImport) {
        importObj.metricName = this.get('importCustomNewMetric');
        importObj.properties = Object.assign(importObj.properties, {
          RRD: this.get('importCustomNewRrd'),
          CONSOLIDATE: this.get('selectedConsolidateOption')
          // TODO support separate FABRIC_GROUP
        });
      }

      // Reset error state for existing field validation
      this.set('isDashboardExistError', false);

      // Check whether provided dashboard name exists before sending onboard requests.
      this.validateDashboardName(isRrdImport, datasetName, fabricGroup)
        .then((isValid) => {
          if (typeof isValid === 'boolean' && isValid) {
            // Begin onboard sequence
            this.processNewImportSequence(isRrdImport, importObj);
            // Disable the form and show options to user
            this.setProperties({
              datasetName,
              isSubmitDone: true,
              isCustomDashFieldDisabled: true
            });
          } else {
            this.setProperties({
              isCustomDashFieldDisabled: true,
              isDashboardExistError: true
            });
          }
        });
    }
  }
});
