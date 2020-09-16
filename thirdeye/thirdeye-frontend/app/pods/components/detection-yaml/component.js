/**
 * Component to render the detection configuration yaml editor.
 * @module components/detection-yaml
 * @property {Number} alertId - alertId needed in edit mode, to submit changes to alert config
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {String} detectionYaml - the detection yaml
 * @property {function} setDetectionYaml - updates the detectionYaml in parent
 * @example
   {{detection-yaml
     alertId=model.alertId
     isEditMode=true
     detectionYaml=detectionYaml
     setDetectionYaml=updateDetectionYaml
   }}
 * @authors lohuynh and hjackson
 */

import Component from '@ember/component';
import { set, get, getProperties, setProperties } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { toastOptions } from 'thirdeye-frontend/utils/constants';
import { defaultDetectionYaml, redundantParse } from 'thirdeye-frontend/utils/yaml-tools';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  selfServeApiGraph, autocompleteAPI
} from 'thirdeye-frontend/utils/api/self-serve';
import {inject as service} from '@ember/service';
import config from 'thirdeye-frontend/config/environment';

export default Component.extend({
  classNames: ['detection-yaml'],
  notifications: service('toast'),
  /**
   * Properties we expect to receive for detection-yaml
   */
  currentMetric: null,
  isYamlParseable: true,
  alertTitle: 'Define detection configuration',
  isEditMode: false,
  disableYamlSave: true,
  detectionYaml: null,                // The YAML for the anomaly detection
  currentYamlAlertOriginal: defaultDetectionYaml,
  alertId: null, // only needed in edit mode
  setDetectionYaml: null, // bubble up detectionYaml changes to parent
  detectionError: false,
  detectionErrorMsg: null,
  detectionErrorInfo: null,
  detectionErrorScroll: false,


  init() {
    this._super(...arguments);
    const {
      detectionYaml,
      currentYamlAlertOriginal
    } = this.getProperties('detectionYaml', 'currentYamlAlertOriginal');
    if (!detectionYaml) {
      set(this, 'detectionYaml', currentYamlAlertOriginal);
    }
  },

  didRender() {
    this._super(...arguments);
    if (this.get('detectionErrorScroll')) {
      document.getElementById("detection-error").scrollIntoView();
      const parentResetScroll = this.get('resetScroll');
      // reset detectionErrorScroll in parent if present, then set in component
      parentResetScroll ? parentResetScroll('detectionErrorScroll') : null;
      set(this, 'detectionErrorScroll', false);
    }
    if (this.get('previewErrorScroll')) {
      document.getElementById("preview-error").scrollIntoView();
      const parentResetScroll = this.get('resetScroll');
      // reset previewErrorScroll in parent if present, then set in component
      parentResetScroll ? parentResetScroll('previewErrorScroll') : null;
      set(this, 'previewErrorScroll', false);
    }
  },

  /**
   * Calls api's for specific metric's autocomplete
   * @method _loadAutocompleteById
   * @return Promise
   */
  _loadAutocompleteById(metricId) {
    const promiseHash = {
      filters: fetch(selfServeApiGraph.metricFilters(metricId)).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(selfServeApiGraph.metricDimensions(metricId)).then(res => checkStatus(res, 'get', true))
    };
    return RSVP.hash(promiseHash);
  },

  /**
   * Get autocomplete suggestions from relevant api
   * @method _buildYamlSuggestions
   * @return Promise
   */
  _buildYamlSuggestions(currentMetric, yamlAsObject, prefix, noResultsArray,
    filtersCache, dimensionsCache, position) {
    // holds default result to return if all checks fail
    let defaultReturn = Promise.resolve(noResultsArray);
    // when metric is being autocompleted, entire text field will be replaced and metricId stored in editor
    if (yamlAsObject.metric === prefix) {
      return fetch(autocompleteAPI.metric(prefix))
        .then(checkStatus)
        .then(metrics => {
          if (metrics && metrics.length > 0) {
            return metrics.map(metric => {
              const [dataset, metricname] = metric.alias.split('::');
              return {
                value: metricname,
                caption: metric.alias,
                row: position.row,
                column: position.column,
                metricname,
                dataset,
                id: metric.id,
                completer:{
                  insertMatch: (editor, data) => {
                    // replace metric row with selected metric
                    editor.session.replace({
                      start: { row: data.row, column: 0 },
                      end: { row: data.row, column: Number.MAX_VALUE }},
                    `metric: ${data.metricname}`);
                    // find dataset: field in text
                    const datasetLocation = editor.find('dataset:');
                    // if found, replace with dataset
                    if (datasetLocation) {
                      editor.session.replace({
                        start: { row: datasetLocation.start.row, column: 0},
                        end: { row: datasetLocation.end.row, column: Number.MAX_VALUE }},
                      `dataset: ${data.dataset}`);
                      // otherwise, add it to the line below the metric field
                    } else {
                      editor.session.insert({
                        row: data.row + 1, column: 0 },
                      `dataset: ${data.dataset}\n`);
                    }
                    editor.metricId = data.id;
                  }
                }};
            });
          }
          return noResultsArray;
        })
        .catch(() => {
          return noResultsArray;
        });
    }
    // if a currentMetric has been stored, we can check autocomplete filters and dimensions
    if (currentMetric) {
      const dimensionValues = yamlAsObject.dimensionExploration.dimensions;
      const filterTypes = typeof yamlAsObject.filters === "object" ? Object.keys(yamlAsObject.filters) : [];
      if (Array.isArray(dimensionValues) && dimensionValues.includes(prefix)) {
        if (dimensionsCache.length > 0) {
          // wraps result in Promise.resolve because return of Promise is expected by yamlSuggestions
          return Promise.resolve(dimensionsCache.map(dimension => {
            return {
              value: dimension
            };
          }));
        }
      }
      let filterKey = '';
      let i = 0;
      while (i < filterTypes.length) {
        if (filterTypes[i] === prefix){
          i = filterTypes.length;
          // wraps result in Promise.resolve because return of Promise is expected by yamlSuggestions
          return Promise.resolve(Object.keys(filtersCache).map(filterType => {
            return {
              value: `${filterType}:`,
              caption: `${filterType}:`,
              snippet: filterType
            };
          }));
        }
        if (Array.isArray(yamlAsObject.filters[filterTypes[i]]) && yamlAsObject.filters[filterTypes[i]].includes(prefix)) {
          filterKey = filterTypes[i];
        }
        i++;
      }
      if (filterKey) {
        // wraps result in Promise.resolve because return of Promise is expected by yamlSuggestions
        return Promise.resolve(filtersCache[filterKey].map(filterParam => {
          return {
            value: filterParam
          };
        }));
      }
    }
    return defaultReturn;
  },

  actions: {
    /**
     * resets given yaml field to default value for creation mode and server value for edit mode
     */
    resetYAML() {
      const setDetectionYaml = get(this, 'setDetectionYaml');
      setDetectionYaml(get(this, 'currentYamlAlertOriginal'));
    },

    /**
     * Closes modal for Preview Error
     */
    togglePreviewModal() {
      set(this, 'showPreviewModal', !get(this, 'showPreviewModal'));
    },

    /**
     * Closes modal for Detection Error
     */
    toggleDetectionModal() {
      set(this, 'showDetectionModal', !get(this, 'showDetectionModal'));
    },

    /**
     * Opens link to detection configuration documentation
     */
    triggerDoc() {
      window.open(config.docs.detectionConfig);
    },

    /**
     * returns array of suggestions for Yaml editor autocompletion
     */
    yamlSuggestions(editor, session, position, prefix) {
      const {
        detectionYaml,
        noResultsArray
      } = getProperties(this, 'detectionYaml', 'noResultsArray');
      let yamlAsObject = {};
      try {
        yamlAsObject = redundantParse(detectionYaml);
        set(this, 'isYamlParseable', true);
      } catch (error) {
        set(this, 'isYamlParseable', false);
        return noResultsArray;
      }
      // if editor.metricId field contains a value, metric was just chosen.  Populate caches for filters and dimensions
      if(editor.metricId){
        const currentMetric = set(this, 'currentMetric', editor.metricId);
        editor.metricId = '';
        return get(this, '_loadAutocompleteById')(currentMetric)
          .then(resultObj => {
            const { filters, dimensions } = resultObj;
            setProperties(this, {
              dimensionsCache: dimensions,
              filtersCache: filters
            });
          })
          .then(() => {
            return get(this, '_buildYamlSuggestions')(currentMetric,
              yamlAsObject, prefix, noResultsArray, get(this, 'filtersCache'),
              get(this, 'dimensionsCache'), position)
              .then(results => results);
          });
      }
      const currentMetric = get(this, 'currentMetric');
      // deals with no metricId, which could be autocomplete for metric or for filters and dimensions already cached
      return get(this, '_buildYamlSuggestions')(currentMetric, yamlAsObject,
        prefix, noResultsArray, get(this, 'filtersCache'),
        get(this, 'dimensionsCache'), position)
        .then(results => results);

    },

    /**
     * Activates 'Create changes' button and stores YAML content in detectionYaml
     */
    onEditingDetectionYamlAction(value) {
      const setDetectionYaml = get(this, 'setDetectionYaml');
      setDetectionYaml(value);
      setProperties(this, {
        detectionMsg: ''
      });
    },

    /**
     * Fired by alert button in YAML UI in edit mode
     * Grabs alert yaml and puts it to the backend.
     */
    async submitAlertEdit() {
      set(this, 'detectionError', false);
      const {
        detectionYaml,
        notifications,
        alertId
      } = getProperties(this, 'detectionYaml', 'notifications', 'alertId');

      //PUT alert
      const alert_url = `/yaml/${alertId}`;
      const alertPostProps = {
        method: 'PUT',
        body: detectionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const alert_result = await fetch(alert_url, alertPostProps);
        const alert_status  = get(alert_result, 'status');
        const alert_json = await alert_result.json();
        if (alert_status !== 200) {
          set(this, 'errorMsg', get(alert_json, 'message'));
          notifications.error(`Failed to save the detection configuration due to: ${alert_json.message}.`, 'Error', toastOptions);
          this.setProperties({
            detectionError: true,
            detectionErrorMsg: alert_json.message,
            detectionErrorInfo: alert_json["more-info"],
            detectionErrorScroll: true
          });
        } else {
          notifications.success('Detection configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving detection config.', error, toastOptions);
        this.setProperties({
          detectionError: true,
          detectionErrorMsg: 'Error while saving detection config.',
          detectionErrorInfo: error,
          detectionErrorScroll: true
        });
      }
    }
  }
});
