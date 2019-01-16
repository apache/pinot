/**
 * Component to render pre-set time range selection pills and a 'custom' one using date-range-picker.
 * @module components/range-pill-selectors
 * @property {Object} timeRangeOptions - object containing our range options
 * @property {Number} timePickerIncrement - determines selectable time increment in date-range-picker
 * @property {Date} activeRangeStart - default start date for range picker
 * @property {Date} activeRangeEnd - default end date for range picker
 * @property {String} uiDateFormat - date format specified by parent route (often specific to metric granularity)
 * @property {Action} selectAction - closure action from parent
 * @example
  {{yaml-editor
    alertTitle="New Editor Title"
    isEditMode=true
    showSettings=true
    onYMLSelector=(action "onYMLSelector")
    saveAlertYaml=(action "saveAlertYaml")
    cancelAlertYaml=(action "cancelAlertYaml")
  }}
 * @author
 */

import Component from '@ember/component';
import { computed, set, get, getProperties } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { yamlAlertProps, yamlAlertSettings, yamIt } from 'thirdeye-frontend/utils/constants';
import yamljs from 'yamljs';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  selfServeApiGraph,
  selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import { inject as service } from '@ember/service';
import { task } from 'ember-concurrency';

export default Component.extend({
  classNames: ['yaml-editor'],
  notifications: service('toast'),
  /**
   * Properties we expect to receive for the yaml-editor
   */
  //isForm: true,
  currentMetric: null,
  isYamlParseable: true,
  alertTitle: 'Define anomaly detection in YAML',
  alertSettingsTitle: 'Define notification settings',
  isEditMode: false,
  showSettings: true,
  disableYamlSave: true,
  errorMsg: '',
  alertYaml: null,           // The YAML for the anomaly alert detection
  detectionSettingsYaml:  null,   // The YAML for the subscription group
  yamlAlertProps: yamlAlertProps,
  yamlAlertSettings: yamlAlertSettings,
  subscriptionGroupNames: [],
  showAnomalyModal: false,
  showNotificationModal: false,
  YAMLField: '',

  /**
   * params passed to yaml-editor component
   */
  async didReceiveAttrs() {
    this._super(...arguments);

    // fetch the subscription group list
    await get(this, '_fetchSubscriptionGroups').perform();
  },

  /**
   * sets Yaml value displayed to contents of alertYaml or yamlAlertProps
   * @method currentYamlValues
   * @return {String}
   */
  currentYamlValues: computed(
    'alertYaml',
    function() {
      const inputYaml = this.get('alertYaml');
      return inputYaml || this.get('yamlAlertProps');
    }
  ),

  /**
   * sets Yaml value displayed to contents of detectionSettingsYaml or yamlAlertSettings
   * @method currentYamlValues
   * @return {String}
   */
  currentYamlSettings: computed(
    'detectionSettingsYaml',
    function() {
      const inputYaml = this.get('detectionSettingsYaml');
      return inputYaml || this.get('yamlAlertSettings');
    }
  ),


  isErrorMsg: computed(
    'errorMsg',
    function() {
      const errorMsg = this.get('errorMsg');
      return errorMsg !== '';
    }
  ),

  _fetchSubscriptionGroups: task(function* () {
    // /detection/subscription-groups
    const url2 = `/detection/subscription-groups`;//dropdown of subscription groups
    const postProps2 = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = get(this, 'notifications');

    try {
      const response = yield fetch(url2, postProps2);
      const json = yield response.json();
      //filter subscription groups with yaml
      set(this, 'subscriptionGroupNames', json.filterBy('yaml'));
    } catch (error) {
      notifications.error('Failed to retrieve subscription groups.', 'Error');
    }
  }).drop(),

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
  _buildYamlSuggestions(currentMetric, yamlAsObject, prefix, noResultsArray, filtersCache, dimensionsCache) {
    // holds default result to return if all checks fail
    let defaultReturn = Promise.resolve(noResultsArray);
    // when metric is being autocompleted, entire text field will be replaced and metricId stored in editor
    if (yamlAsObject.metric === prefix) {
      return fetch(selfServeApiCommon.metricAutoComplete(prefix))
        .then(checkStatus)
        .then(metrics => {
          if (metrics && metrics.length > 0) {
            return metrics.map(metric => {
              const [dataset, metricname] = metric.alias.split('::');
              return {
                value: metricname,
                caption: metric.alias,
                metricname,
                dataset,
                id: metric.id,
                completer:{
                  insertMatch: (editor, data) => {
                    editor.setValue(yamIt(data.metricname, data.dataset));
                    editor.metricId = data.id;
                    //editor.completer.insertMatch({value: data.value});
                    // editor.insert('abc');
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
     * resets given yaml field to default value
     */
    resetYAML(field) {
      if (field === 'anomaly') {
        const yamlAlertProps = get(this, 'yamlAlertProps');
        set(this, 'alertYaml', yamlAlertProps);
      } else if (field === 'notification') {
        const yamlAlertSettings = get(this, 'yamlAlertSettings');
        set(this, 'detectionSettingsYaml', yamlAlertSettings);
      }
    },

    /**
     * Brings up appropriate modal, based on which yaml field is clicked
     */
    triggerDocModal(field) {
      set(this, `show${field}Modal`, true);
      set(this, 'YAMLField', field);
    },

    /**
     * Updates the notification settings yaml with user section
     */
    onYAMLGroupSelectionAction(value) {
      if(value.yaml) {
        set(this, 'currentYamlSettings', value.yaml);
        set(this, 'groupName', value);
      }
    },

    /**
     * returns array of suggestions for Yaml editor autocompletion
     */
    yamlSuggestions(editor, session, position, prefix) {
      const {
        alertYaml,
        noResultsArray
      } = getProperties(this, 'alertYaml', 'noResultsArray');
      let yamlAsObject = {};
      try {
        yamlAsObject = yamljs.parse(alertYaml);
        set(this, 'isYamlParseable', true);
      }
      catch(err){
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
            this.setProperties({
              dimensionsCache: dimensions,
              filtersCache: filters
            });
          })
          .then(() => {
            return get(this, '_buildYamlSuggestions')(currentMetric, yamlAsObject, prefix, noResultsArray, get(this, 'filtersCache'), get(this, 'dimensionsCache'))
              .then(results => results);
          });
      }
      const currentMetric = get(this, 'currentMetric');
      // deals with no metricId, which could be autocomplete for metric or for filters and dimensions already cached
      return get(this, '_buildYamlSuggestions')(currentMetric, yamlAsObject, prefix, noResultsArray, get(this, 'filtersCache'), get(this, 'dimensionsCache'))
        .then(results => results);

    },

    /**
     * Activates 'Create Alert' button and stores YAML content in alertYaml
     */
    onYMLSelectorAction(value) {
      set(this, 'disableYamlSave', false);
      set(this, 'alertYaml', value);
      set(this, 'errorMsg', '');
    },

    /**
     * Activates 'Create Alert' button and stores YAML content in detectionSettingsYaml
     */
    onYMLSettingsSelectorAction(value) {
      set(this, 'disableYamlSave', false);
      set(this, 'detectionSettingsYaml', value);
    },

    cancelAlertYamlAction() {
      //call the onConfirm property to invoke the passed in action
      get(this, 'cancelAlertYaml')();
    },

    /**
     * Fired by save button in YAML UI
     * Grabs YAML content and sends it
     */
    saveAlertYamlAction() {
      const content = {
        detection: get(this, 'alertYaml'),
        notification: get(this, 'currentYamlSettings')
      };
      const url = '/yaml/create-alert';
      const postProps = {
        method: 'post',
        body: JSON.stringify(content),
        headers: { 'content-type': 'application/json' }
      };
      const notifications = this.get('notifications');

      fetch(url, postProps).then((res) => {
        res.json().then((result) => {
          if (result && result.message) {
            set(this, 'errorMsg', result.message);
          }
          if (result.detectionAlertConfigId && result.detectionConfigId) {
            notifications.success('Save alert yaml successfully.', 'Saved');
          }
        });

      }).catch((error) => {
        notifications.error('Save alert yaml file failed.', error);
      });
    }
  }
});
