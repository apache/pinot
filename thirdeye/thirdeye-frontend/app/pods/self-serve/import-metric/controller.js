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
  isSubmitDisabled: true,
  isMetricOnboarded: false,
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
    'importCustomNewRrd', {
    get() {
      const rrd = this.get('importCustomNewRrd');
      const name = this.get('importCustomNewDataset');
      const metric = this.get('importCustomNewMetric');
      return Ember.isPresent(rrd) || Ember.isPresent(name) || Ember.isPresent(metric);
    },
    set(value) {
      return value;
    }
  }),

  /**
   * Determines whether all fields are disabled (after submit)
   * @method isFormDisabled
   * @return {Boolean} isFormDisabled
   */
  isFormDisabled : Ember.computed.and('isExistingDashFieldDisabled', 'isCustomDashFieldDisabled'),

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  fetchMetricsList(dataSet) {
    const url = `/thirdeye-admin/metric-config/metrics?dataset=${dataSet}`
    return fetch(url).then(res => res.json());
  },

  /**
   * Triggers instant onboard for the generated metrics. If not triggered manually here,
   * the onboard service runs every 15 minutes anyway.
   * @method triggerInstantOnboard
   * @return {Ember.RSVP.Promise}
   */
  triggerInstantOnboard() {
    const url = '/autoOnboard/runAdhoc/AutometricsThirdeyeDataSource';
    return new Ember.RSVP.Promise((resolve) => {
      fetch(url, { method: 'post' }).then(res => resolve(checkStatus(res)));
    });
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
    return new Ember.RSVP.Promise((resolve) => {
      fetch(url, postProps).then(res => resolve(checkStatus(res)));
    });
  },

  /**
   * Generates/updates the metrics for an existing inGraphs dashboard
   * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Onboarding+ingraph+dashboards
   * @method onboardMetricsToDataset
   * @param {String} rrd - The metric RRD
   * @return {Ember.RSVP.Promise}
   */
  onboardMetricsToDataset(rrd) {
    const postProps = {
      method: 'post',
      body: JSON.stringify({ RRD: rrd }),
      headers: { 'content-type': 'Application/Json' }
    };
    const url = '/onboard/create';
    return new Ember.RSVP.Promise((resolve) => {
      fetch(url, postProps).then(res => resolve(checkStatus(res)));
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
       isExistingDashFieldDisabled: false,
       isImportSuccess: false,
       isImportError: false,
       isMetricOnboarded: false,
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

      // Disable the form and show options to user
      this.setProperties({
        datasetName,
        isCustomDashFieldDisabled: true,
        isExistingDashFieldDisabled: true
      });

      // Make import request
      this.onboardNewDataset(importObj).then(importResult => {
        // Check server for newly onboarded metrics
        this.fetchMetricsList(datasetName).then(metricsList => {
          if (!metricsList.Records.length) {
            this.set('isImportError', true);
          } else {
            // Trigger onboard for imported metrics
            this.triggerInstantOnboard().then(onboardRes => {
              this.setProperties({
                isImportSuccess: true,
                isMetricOnboarded: true,
                importedMetrics: metricsList.Records
              });
            });
          }
        });
      });
    }
  }
});
