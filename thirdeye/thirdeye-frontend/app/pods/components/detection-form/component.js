/**
 * Component to render the detection configuration form editor.
 * @module components/detection-form
 * @property {boolean} isEditMode - flag to control submit button and method used for submiting changes
 * @property {String} detectionYaml - yaml configuration of detection
 * @property {Function} setDetectionYaml - action for sending yaml updates to parent
 * @property {Function} changeGeneralFieldsEnabled - action for enabling subsequent fields
 * @property {boolean} generalFieldsEnabled - flag to disable boxes for input flow control
 * @example
   {{detection-form
     isEditMode=false
     detectionYaml=detectionYaml
     setDetectionYaml=(action "updateDetectionYaml")
     changeGeneralFieldsEnabled=(action "changeGeneralFieldsEnabled")
     generalFieldsEnabled=generalFieldsEnabled
   }}
 * @author hjackson
 */

import Component from '@ember/component';
import { computed, set, get } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { defaultDetectionYaml, fieldsToYaml, redundantParse } from 'thirdeye-frontend/utils/yaml-tools';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  selfServeApiGraph, selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import config from 'thirdeye-frontend/config/environment';
import { task, timeout } from 'ember-concurrency';

export default Component.extend({
  metricHelpMailto: `mailto:${config.email}?subject=Metric Onboarding Request (non-additive UMP or derived)`,
  isEditMode: false, // passed in by parent
  metricLookupCache: [],
  selectedDimension: null,
  selectedMetric: null, // set using yaml or metric dropdown
  selectedApplication: null,
  selectedDataset: null, //set using Yaml or by selecting metric with form
  detectionYaml: null, // shared with detection-yaml component - passed in by parent
  setDetectionYaml: null, // action passed in by parent
  detectionName: null, // set using yaml or input box
  isDetectionNameUserModified: false, // set true if name passed in through yaml or typed
  isDetectionNameDuplicate: false, // set true if name not valid
  generalFieldsEnabled: false, // controlled by parent, do not set


  /**
   * Generate alert name primer based on user selections
   * @type {String}
   */
  detectionNamePrimer: computed(
    'selectedDimension',
    'selectedMetric',
    'selectedDataset',
    function() {
      const {
        selectedDataset,
        selectedDimension,
        selectedMetric
      } = this.getProperties(
        'selectedDataset',
        'selectedDimension',
        'selectedMetric'
      );
      const dataset = selectedDataset ? `${selectedDataset.camelize()}_` : 'datasetName_';
      const dimension = selectedDimension ? `${selectedDimension.camelize()}_` : '';
      const metric = selectedMetric ? `${selectedMetric.name.camelize()}_` : 'metricName_';
      return `${dataset}${metric}${dimension}`;
    }
  ),

  updatedFields: computed(
    'detectionName',
    'selectedMetric',
    'selectedDataset',
    function() {
      const {
        detectionName,
        selectedMetric,
        selectedDataset
      } = this.getProperties('detectionName', 'selectedMetric', 'selectedDataset');
      return (detectionName || selectedMetric || selectedDataset);
    }
  ),

  /**
   * Handler for search by function name - using ember concurrency (task)
   * @method searchMetricsList
   * @param {metric} String - portion of metric name used in typeahead
   * @return {Promise}
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const autoCompleteResults = yield fetch(selfServeApiCommon.metricAutoComplete(metric)).then(checkStatus);
    this.get('metricLookupCache').push(...autoCompleteResults);
    return autoCompleteResults;
  }),

  init() {
    this._super(...arguments);
    const detectionYaml = get(this, 'detectionYaml');
    if (detectionYaml && detectionYaml !== defaultDetectionYaml) {
      this._getFieldsFromYaml(detectionYaml);
    }
  },

  /**
   * This hook will integrate form changes with yaml and send the new yaml to parent
   * so that the changes are reflected in detection-yaml
   * @method willDestroyElement
   * @return (undefined)
   */
  willDestroyElement() {
    // don't need to run this unless there are new fields
    if (get(this, 'updatedFields')){
      const fields = {
        detectionName: this.get('detectionName'),
        metric: this.get('selectedMetric') ? this.get('selectedMetric').name : null,
        dataset: this.get('selectedDataset')
        // to-do: put rules in here after rule component is operational
      };
      // send fields and yaml to yaml util to merge old to update values
      let detectionYaml = get(this, 'detectionYaml') ? get(this, 'detectionYaml') : defaultDetectionYaml;
      detectionYaml = fieldsToYaml(fields, detectionYaml);
      // send new Yaml to parent
      this.get('setDetectionYaml')(detectionYaml);
    }
  },

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
   * If the yaml is passed in, then parse the relevant fields to props for the form
   * Assumes yaml string is not the default detection yaml
   * @method _getFieldsFromYaml
   * @param {String} yaml - current yaml string
   * @return {undefined}
   */
  _getFieldsFromYaml(yaml) {
    let yamlAsObject = {};
    let dy = {}; // dy = default yaml
    let metricAsObject;
    let metricAlias;
    let dataset;
    try {
      yamlAsObject = redundantParse(yaml);
      dy = redundantParse(defaultDetectionYaml);
      set(this, 'isYamlParseable', true);
    } catch(err) {
      set(this, 'isYamlParseable', false);
      return null;
    }
    // if dataset and metric are in the yaml and are not default template values, convert them to alias
    if (yamlAsObject.dataset && yamlAsObject.metric && yamlAsObject.dataset !== dy.dataset && yamlAsObject.metric !== dy.metric) {
      metricAlias = `${yamlAsObject.dataset}::${yamlAsObject.metric}`;
    }
    // if the alias is there, we can build selectedMetric and populate the dropdown
    if (metricAlias) {
      metricAsObject = {
        alias: metricAlias,
        name: yamlAsObject.metric
      };
      dataset = yamlAsObject.dataset;
      get(this, 'changeGeneralFieldsEnabled')('isMetricSelected', true);
    }
    // we only use fields that have been updated
    this.setProperties({
      selectedDataset: dataset,
      selectedMetric: metricAsObject,
      detectionDescription: (yamlAsObject.detectionDescription !== dy.detectionDescription) ? yamlAsObject.detectionDescription: null,
      rulesOfDetection: (yamlAsObject.rules !== dy.rules) ? yamlAsObject.rules: null
    });
    // if the user has already specified a name, keep it
    if (yamlAsObject.detectionName !== dy.detectionName) {
      this.setProperties({
        detectionName: yamlAsObject.detectionName,
        isDetectionNameUserModified: true
      });
    // otherwise, generate one if there is a metric alias available
    } else if (metricAlias){
      this._modifyDetectionName();
    }
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * Note: these requests can fail silently and any empty response will fall back on defaults.
   * @method _fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {RSVP.promise}
   */
  _fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(selfServeApiGraph.maxDataTime(metricId)).then(res => checkStatus(res, 'get', true)),
      filters: fetch(selfServeApiGraph.metricFilters(metricId)).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(selfServeApiGraph.metricDimensions(metricId)).then(res => checkStatus(res, 'get', true))
    };
    return RSVP.hash(promiseHash);
  },

  /**
   * Auto-generate the alert name until the user directly edits it
   * @method _modifyDetectionName
   * @return {undefined}
   */
  _modifyDetectionName() {
    const {
      detectionNamePrimer,
      isDetectionNameUserModified
    } = this.getProperties('detectionNamePrimer', 'isDetectionNameUserModified');
    // If user has not yet edited the alert name, continue to auto-generate it.
    if (!isDetectionNameUserModified) {
      this.set('detectionName', detectionNamePrimer);
    }
    // Each time we modify the name, we validate it as well to ensure no duplicates exist.
    this._validateDetectionName(this.get('detectionName'));
  },

  /**
   * Make sure alert name does not already exist in the system
   * @method _validateDetectionName
   * @param {String} userProvidedName - The new alert name
   * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
   * If user-modified, we will stop modifying it dynamically (via 'isDetectionNameUserModified')
   * @return {undefined}
   */
  _validateDetectionName(userProvidedName, userModified = false) {
    this._fetchAlertsByName(userProvidedName).then(matchingAlerts => {
      const isDuplicateName = matchingAlerts.find(alert => alert.functionName === userProvidedName);
      // If the user edits the alert name, we want to stop auto-generating it.
      if (userModified) {
        this.set('isDetectionNameUserModified', true);
      }
      // Either add or clear the "is duplicate name" banner
      this.set('isDetectionNameDuplicate', isDuplicateName);
    });
  },



  actions: {
    /**
     * When a metric is selected, fetch its props, and send them to the graph builder
     * TODO: if 'hash.dimensions' is not needed, lets refactor the RSVP object instead of renaming
     * @method onSelectMetric
     * @param {Object} selectedObj - The selected metric
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      this.setProperties({
        isMetricDataLoading: true,
        selectedMetric: selectedObj,
        selectedDataset: selectedObj.alias.split('::')[0] // grab dataset name
      });
      get(this, 'changeGeneralFieldsEnabled')('isMetricSelected', true);
      this._fetchMetricData(selectedObj.id)
        .then((metricHash) => {
          const { maxTime, filters, dimensions } = metricHash;

          this.setProperties({
            maxTime,
            filters,
            dimensions,
            metricLookupCache: [],
            originalDimensions: dimensions,
            detectionName: this.get('detectionNamePrimer')
          });
        })
        .catch((err) => {
          this.setProperties({
            isSelectMetricError: true,
            selectMetricErrMsg: err
          });
        });
    },

    /**
     * Bubble up to validateDetectionName action
     * @method validateName
     * @param {String} userProvidedName - The new alert name
     * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
     * @return {undefined}
     */
    validateName(userProvidedName, userModified = false) {
      this._validateDetectionName(userProvidedName, userModified);
    },

    /**
     * When a dimension is selected, fetch new anomaly graph data based on that dimension
     * and trigger a new graph load, showing the top contributing subdimensions.
     * @method onSelectDimension
     * @param {Object} selectedDimension - The selected dimension to apply
     * @return {undefined}
     */
    onSelectDimension(selectedDimension) {
      this.setProperties({
        selectedDimension,
        isDimensionFetchDone: false
      });
      if (selectedDimension === 'All') {
        this.setProperties({
          topDimensions: [],
          isSecondaryDataLoading: false
        });
      } else {
        this.set('isSecondaryDataLoading', true);
        this._modifyDetectionName();
      }
    }
  }
});
