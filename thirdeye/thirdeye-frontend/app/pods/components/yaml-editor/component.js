/**
 * Component to render the alert and subscription group yaml editors.
 * @module components/yaml-editor
 * @property {number} alertId - the alert id
 * @property {number} subscriptionGroupId - the subscription group id
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {boolean} showSettings - to show the subscriber groups yaml editor
 * @property {Object} subscriptionGroupNames - the list of subscription groups
 * @property {Object} detectionYaml - the detection yaml to display
 * @property {Object} subscriptionYaml - the subscription group yaml to display
 * @example
   {{yaml-editor
     alertId=model.alertId
     subscriptionGroupId=model.subscriptionGroupId
     isEditMode=true
     showSettings=true
     subscriptionGroupNames=model.subscriptionGroupNames
     detectionYaml=model.detectionYaml
     subscriptionYaml=model.subscriptionYaml
   }}
 * @author lohuynh
 */

import Component from '@ember/component';
import {computed, set, get, getProperties, setProperties} from '@ember/object';
import {checkStatus} from 'thirdeye-frontend/utils/utils';
import {yamlAlertProps, yamlAlertSettings, toastOptions} from 'thirdeye-frontend/utils/constants';
import yamljs from 'yamljs';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  selfServeApiGraph, selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import {inject as service} from '@ember/service';
import {task} from 'ember-concurrency';
import config from 'thirdeye-frontend/config/environment';

export default Component.extend({
  classNames: ['yaml-editor'],
  notifications: service('toast'),
  /**
   * Properties we expect to receive for the yaml-editor
   */
  currentMetric: null,
  isYamlParseable: true,
  alertTitle: 'Define detection configuration',
  alertSettingsTitle: 'Define subscription configuration',
  isEditMode: false,
  showSettings: true,
  disableYamlSave: true,
  detectionMsg: '',                   //General alert failures
  subscriptionMsg: '',                //General subscription failures
  detectionYaml: null,                // The YAML for the anomaly detection
  subscriptionYaml:  null,            // The YAML for the subscription group
  yamlAlertProps: yamlAlertProps,
  yamlAlertSettings: yamlAlertSettings,
  showAnomalyModal: false,
  showNotificationModal: false,
  currentYamlAlertOriginal: '',
  currentYamlSettingsOriginal: '',
  toggleCollapsed: true,
  alertDataIsCurrent: true,



  init() {
    this._super(...arguments);
    // In edit mode, sets subscription group to an existing group by default and sets default yamls
    if (get(this, 'isEditMode')) {
      const subscriptionGroupNames = get(this, 'subscriptionGroupNames');
      // Checks to make sure there is a subscription group array with at least one subscription group
      if (subscriptionGroupNames && Array.isArray(subscriptionGroupNames) && subscriptionGroupNames.length > 0) {
        const firstGroup = subscriptionGroupNames[0];
        set(this, 'subscriptionYaml', firstGroup.yaml);
        set(this, 'groupName', firstGroup);
        set(this, 'subscriptionGroupId', firstGroup.id);
      }
      // Sets default yamls after checking for and setting default subscription group
      set(this, 'currentYamlAlertOriginal', get(this, 'detectionYaml') || get(this, 'yamlAlertProps'));
      set(this, 'currentYamlSettingsOriginal', get(this, 'subscriptionYaml') || get(this, 'yamlAlertSettings'));
    }
  },

  /**
   * populates subscription group dropdown with options from fetch or model
   * @method subscriptionGroupNamesDisplay
   * @return {Object}
   */
  subscriptionGroupNamesDisplay: computed(
    'subscriptionGroupNames',
    async function() {
      const isEditMode = get(this, 'isEditMode');
      if (isEditMode) {
        return get(this, 'subscriptionGroupNames');
      }
      const subscriptionGroups = await get(this, '_fetchSubscriptionGroups').perform();
      return get(this, 'subscriptionGroupNames') || subscriptionGroups;
    }
  ),

  /**
   * sets Yaml value displayed to contents of detectionYaml or yamlAlertProps
   * @method currentYamlAlert
   * @return {String}
   */
  currentYamlAlert: computed(
    'detectionYaml',
    function() {
      const inputYaml = get(this, 'detectionYaml');
      return inputYaml || get(this, 'yamlAlertProps');
    }
  ),

  /**
   * sets Yaml value displayed to contents of subscriptionYaml or yamlAlertSettings
   * @method currentYamlAlert
   * @return {String}
   */
  currentSubscriptionYaml: computed(
    'subscriptionYaml',
    function() {
      const subscriptionYaml = get(this, 'subscriptionYaml');
      return subscriptionYaml || get(this, 'yamlAlertSettings');
    }
  ),


  isDetectionMsg: computed(
    'detectionMsg',
    function() {
      const detectionMsg = get(this, 'detectionMsg');
      return detectionMsg !== '';
    }
  ),

  isSubscriptionMsg: computed(
    'subscriptionMsg',
    function() {
      const subscriptionMsg = get(this, 'subscriptionMsg');
      return subscriptionMsg !== '';
    }
  ),

  _fetchSubscriptionGroups: task(function* () {
    //dropdown of subscription groups
    const url2 = `/detection/subscription-groups`;
    const postProps2 = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = get(this, 'notifications');

    try {
      const response = yield fetch(url2, postProps2);
      const json = yield response.json();
      return json.filterBy('yaml');
    } catch (error) {
      notifications.error('Failed to retrieve subscription groups.', 'Error', toastOptions);
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
  _buildYamlSuggestions(currentMetric, yamlAsObject, prefix, noResultsArray,
    filtersCache, dimensionsCache, position) {
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
    changeAccordion() {
      set(this, 'toggleCollapsed', !get(this, 'toggleCollapsed'));
    },

    /**
     * resets given yaml field to default value for creation mode and server value for edit mode
     */
    resetYAML(field) {
      const isEditMode = get(this, 'isEditMode');
      if (field === 'anomaly') {
        if(isEditMode) {
          set(this, 'detectionYaml', get(this, 'currentYamlAlertOriginal'));
        } else {
          const yamlAlertProps = get(this, 'yamlAlertProps');
          set(this, 'detectionYaml', yamlAlertProps);
        }
      } else if (field === 'subscription') {
        if(isEditMode) {
          set(this, 'subscriptionYaml', get(this, 'currentYamlSettingsOriginal'));
        } else {
          const yamlAlertSettings = get(this, 'yamlAlertSettings');
          set(this, 'subscriptionYaml', yamlAlertSettings);
        }
      }
    },

    /**
     * Brings up appropriate modal, based on which yaml field is clicked
     */
    triggerDoc(field) {
      if (field === 'Anomaly') {
        window.open(config.docs.detectionConfig);
      } else {
        window.open(config.docs.subscriptionConfig);
      }
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
        yamlAsObject = yamljs.parse(detectionYaml);
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
      setProperties(this, {
        disableYamlSave: false,
        detectionYaml: value,
        detectionMsg: '',
        subscriptionMsg: '',
        alertDataIsCurrent: false
      });
    },

    /**
     * Activates 'Create changes' button and stores YAML content in subscriptionYaml
     */
    onEditingSubscriptionYamlAction(value) {
      setProperties(this, {
        disableYamlSave: false,
        subscriptionYaml: value
      });
    },

    /**
     * Updates the subscription settings yaml with user section
     */
    onSubscriptionGroupSelectionAction(value) {
      if(value.yaml) {
        set(this, 'subscriptionYaml', value.yaml);
        set(this, 'groupName', value);
        set(this, 'subscriptionGroupId', value.id);
      }
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
    },

    /**
     * Fired by save button in YAML UI
     * Grabs each yaml (alert and settings) and save them to their respective apis.
     */
    async saveEditYamlAction() {
      const {
        detectionYaml,
        subscriptionYaml,
        notifications,
        alertId,
        subscriptionGroupId
      } = getProperties(this, 'detectionYaml', 'subscriptionYaml', 'notifications', 'alertId', 'subscriptionGroupId');

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
        } else {
          notifications.success('Detection configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving detection config.', error, toastOptions);
      }
      //PUT settings
      const setting_url = `/yaml/subscription/${subscriptionGroupId}`;
      const settingsPostProps = {
        method: 'PUT',
        body: subscriptionYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const settings_result = await fetch(setting_url, settingsPostProps);
        const settings_status  = get(settings_result, 'status');
        const settings_json = await settings_result.json();
        if (settings_status !== 200) {
          set(this, 'errorMsg', get(settings_json, 'message'));
          notifications.error(`Failed to save the subscription configuration due to: ${settings_json.message}.`, 'Error', toastOptions);
        } else {
          notifications.success('Subscription configuration saved successfully', 'Done', toastOptions);
        }
      } catch (error) {
        notifications.error('Error while saving subscription config.', error, toastOptions);
      }
    }
  }
});
