/**
 * Handles metrics import from inGrahps dashboards
 * @module self-serve/create/import-metric
 * @exports import-metric
 */
import Ember from 'ember';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Controller.extend({
  /**
   * Import Defaults
   */
  isImportSuccess: false,
  isImportError: false,
  isSubmitDone: false,
  isSubmitDisabled: true,
  isDashboardExistError: false,
  isExistingDashFieldDisabled: false,
  isCustomDashFieldDisabled: false,

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @return {Boolean} isDisabled
   */
  isSubmitDisabled: Ember.computed(
    'importExistingDashboardName',
    'importCustomNewDataset',
    'importCustomNewMetric',
    'importCustomNewRrd',
    function() {
      let isDisabled = true;
      const existingNameField = this.get('importExistingDashboardName');
      const newNameField = this.get('importCustomNewDataset');
      const newMetricField = this.get('importCustomNewMetric');
      const newRrdField = this.get('importCustomNewRrd');
      // If existing dashboard field is filled, release submit button.
      if (Ember.isPresent(existingNameField)) {
        isDisabled = false;
      }
      // If any of the 'import custom' fields are filled, assume user will go the RRD import route. Disable submit.
      if (Ember.isPresent(newNameField) || Ember.isPresent(newRrdField) || Ember.isPresent(newMetricField)) {
        isDisabled = true;
        // Enable submit if all required RRD fields are present
        if (Ember.isPresent(newNameField) && Ember.isPresent(newRrdField) && Ember.isPresent(newMetricField)) {
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
  isExistingDashFieldDisabled: Ember.computed(
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
      return Ember.isPresent(rrd) || Ember.isPresent(name) || Ember.isPresent(metric) || isSubmitted;
    }
  ),

  /**
   * Determines whether all fields are disabled (after submit)
   * @method isFormDisabled
   * @return {Boolean} isFormDisabled
   */
  isFormDisabled: Ember.computed.and('isExistingDashFieldDisabled', 'isCustomDashFieldDisabled'),

  /**
   * Validates whether the entered dashboard name exists in inGraphs
   * @method validateDashboardName
   * @param {Boolean} isRrdImport - indicates whether we are importing custom dashboard name
   * @param {String} dashboardName - name of dashboard to import
   * @return {Promise}
   */
  validateDashboardName(isRrdImport, dashboardName) {
    // TODO: add correct endpoint
    const url = `/onboard/validate?dashboard=${dashboardName}`
    // Only make the call if we are importing an existing inGraphs dashboard (isRrdImport = false)
    if (isRrdImport) {
      return Promise.resolve(true);
    } else {
      // TODO: enable when endpoint ready
      // return fetch(url).then((res) => res.json());
      return Promise.resolve(true);
    }
  },

  /**
   * Fetches a list of existing metrics by dataset name
   * @method fetchMetricsList
   * @param {String} dataSet - name of dataset to use in lookup
   * @return {Promise}
   */
  fetchMetricsList(dataSet) {
    const url = `/thirdeye-admin/metric-config/metrics?dataset=${dataSet}`
    return fetch(url).then((res) => res.json());
  },

  /**
   * Triggers instant onboard for the generated metrics. If not triggered manually here,
   * the onboard service runs every 15 minutes anyway.
   * @method triggerInstantOnboard
   * @return {Ember.RSVP.Promise}
   */
  triggerInstantOnboard() {
    const url = '/autoOnboard/runAdhoc/AutometricsThirdeyeDataSource';
    return fetch(url, { method: 'post' }).then(checkStatus);
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
    const url = '/onboard/create';
    return fetch(url, postProps).then(checkStatus);
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
    // Check for metrics in TE that belong to the new dataset name
    .then((onboardResult) => {
      return this.fetchMetricsList(importObj.datasetName);
    })
    // If this is a custom dashboard import, and no metrics returned, assume something went wrong.
    .then((metricsList) => {
      if (isRrdImport && !metricsList.Records.length) {
        return new Promise.reject();
      } else {
        this.setProperties({
          isImportSuccess: true,
          importedMetrics: metricsList.Records
        });
      }
    })
    .catch((error) => {
      this.set('isImportError', true);
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
       importCustomNewDataset: '',
       importCustomNewMetric: '',
       importCustomNewRrd: '',
       datasetName: '',
      });
    },

    /**
     * Handles form submit
     * @method submit
     * @return {undefined}
     */
    submit() {
      const isRrdImport = this.get('isExistingDashFieldDisabled');
      const datasetName = isRrdImport ? this.get('importCustomNewDataset') : this.get('importExistingDashboardName');
      const importObj = { datasetName: datasetName, dataSource: 'AutometricsThirdeyeDataSource' };

      // Enhance request payload for custom metrics
      if (isRrdImport) {
        importObj.metricName = this.get('importCustomNewMetric');
        importObj.properties = { RRD: this.get('importCustomNewRrd') };
      }

      // Reset error state for existing field validation
      this.set('isDashboardExistError', false);

      // Check whether provided dashboard name exists before sending onboard requests.
      this.validateDashboardName(isRrdImport, datasetName)
      .then((isValid) => {
        if (isValid) {
          // Begin onboard sequence
          this.processNewImportSequence(isRrdImport, importObj);
          // Disable the form and show options to user
          this.setProperties({
            datasetName,
            isSubmitDone: true,
            isCustomDashFieldDisabled: true
          });
        } else {
          this.set('isCustomDashFieldDisabled', true);
          this.set('isDashboardExistError', true);
        }
      })
      .catch((error) => {
        this.set('isImportError', true);
      });
    }
  }
});
