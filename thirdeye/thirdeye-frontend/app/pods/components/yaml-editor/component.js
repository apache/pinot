/**
 * Component to render the alert and subscription group yaml editors.
 * @module components/yaml-editor
 * @property {number} alertId - the alert id
 * @property {number} subscriptionGroupId - the subscription group id
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {boolean} showSettings - to show the subscriber groups yaml editor
 * @property {Object} subscriptionGroupNames - the list of subscription groups
 * @property {Object} alertYaml - the alert yaml to display
 * @property {Object} detectionSettingsYaml - the subscription group yaml to display
 * @example
   {{yaml-editor
     alertId=model.alertId
     subscriptionGroupId=model.subscriptionGroupId
     isEditMode=true
     showSettings=true
     subscriptionGroupNames=model.detectionSettingsYaml
     alertYaml=model.detectionYaml
     detectionSettingsYaml=model.detectionSettingsYaml
   }}
 * @author lohuynh
 */

import Component from '@ember/component';
import { computed, set, get, getProperties } from '@ember/object';
import { later } from '@ember/runloop';
import { checkStatus, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import { colorMapping, toColor, makeTime } from 'thirdeye-frontend/utils/rca-utils';
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
import moment from 'moment';
import _ from 'lodash';
import d3 from 'd3';

const PREVIEW_DATE_FORMAT = 'MMM DD, hh:mm a';

export default Component.extend({
  classNames: ['yaml-editor'],
  notifications: service('toast'),
  /**
   * Properties we expect to receive for the yaml-editor
   */
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
  showAnomalyModal: false,
  showNotificationModal: false,
  YAMLField: '',
  currentYamlAlertOriginal: '',
  currentYamlSettingsOriginal: '',
  toggleCollapsed: true,
  timeseries: null,
  isLoading: false,
  analysisRange: [moment().subtract(1, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],
  displayRange: [moment().subtract(2, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],
  colorMapping: colorMapping,
  zoom: {
    enabled: true,
    rescale: true
  },

  legend: {
    show: true,
    position: 'right'
  },
  errorTimeseries: null,
  metricUrn: null,
  errorBaseline: null,
  compareMode: 'wo1w',
  baseline: null,
  errorAnomalies: null,
  showPreview: false,
  componentId: 'timeseries-chart',



  init() {
    this._super(...arguments);

    if(get(this, 'isEditMode')) {
      set(this, 'currentYamlAlertOriginal', get(this, 'alertYaml') || get(this, 'yamlAlertProps'));
      set(this, 'currentYamlSettingsOriginal', get(this, 'detectionSettingsYaml') || get(this, 'yamlAlertSettings'));
    }
  },

  disablePreviewButton: computed(
    'alertYaml',
    'isLoading',
    function() {
      return (get(this, 'alertYaml') === null || get(this, 'isLoading') === true);
    }
  ),

  axis: computed(
    'displayRange',
    function () {
      const displayRange = getProperties(this, 'displayRange');

      return {
        y: {
          show: true,
          tick: {
            format: function(d){return humanizeFloat(d);}
          }
        },
        y2: {
          show: false,
          min: 0,
          max: 1
        },
        x: {
          type: 'timeseries',
          show: true,
          min: displayRange[0],
          max: displayRange[1],
          tick: {
            fit: false,
            format: (d) => {
              const t = makeTime(d);
              if (t.valueOf() === t.clone().startOf('day').valueOf()) {
                return t.format('MMM D (ddd)');
              }
              return t.format('h:mm a');
            }
          }
        }
      };
    }
  ),

  series: computed(
    'anomalies',
    'timeseries',
    'baseline',
    'analysisRange',
    'displayRange',
    function () {
      const metricUrn = get(this, 'metricUrn');
      const anomalies = get(this, 'anomalies');
      const timeseries = get(this, 'timeseries');
      const baseline = get(this, 'baseline');
      const analysisRange = get(this, 'analysisRange');
      const displayRange = get(this, 'displayRange');

      const series = {};

      if (!_.isEmpty(anomalies)) {

        anomalies
          .filter(anomaly => anomaly.metricUrn === metricUrn)
          .forEach(anomaly => {
            const key = this._formatAnomaly(anomaly);
            series[key] = {
              timestamps: [anomaly.startTime, anomaly.endTime],
              values: [1, 1],
              type: 'line',
              color: 'teal',
              axis: 'y2'
            };
            series[key + '-region'] = Object.assign({}, series[key], {
              type: 'region',
              color: 'orange'
            });
          });
      }

      if (timeseries && !_.isEmpty(timeseries.value)) {
        series['current'] = {
          timestamps: timeseries.timestamp,
          values: timeseries.value,
          type: 'line',
          color: toColor(metricUrn)
        };
      }

      if (baseline && !_.isEmpty(baseline.value)) {
        series['baseline'] = {
          timestamps: baseline.timestamp,
          values: baseline.value,
          type: 'line',
          color: 'light-' + toColor(metricUrn)
        };
      }

      // detection range
      if (timeseries && !_.isEmpty(timeseries.value)) {
        series['pre-detection-region'] = {
          timestamps: [displayRange[0], analysisRange[0]],
          values: [1, 1],
          type: 'region',
          color: 'grey'
        };
      }

      return series;
    }
  ),

  /**
   * sets Yaml value displayed to contents of alertYaml or yamlAlertProps
   * @method currentYamlAlert
   * @return {String}
   */
  subscriptionGroupNamesDisplay: computed(
    'subscriptionGroupNames',
    async function() {
      const subscriptionGroups = await get(this, '_fetchSubscriptionGroups').perform();
      return get(this, 'subscriptionGroupNames') || subscriptionGroups;
    }
  ),

  /**
   * sets Yaml value displayed to contents of alertYaml or yamlAlertProps
   * @method currentYamlAlert
   * @return {String}
   */
  currentYamlAlert: computed(
    'alertYaml',
    function() {
      const inputYaml = get(this, 'alertYaml');
      return inputYaml || get(this, 'yamlAlertProps');
    }
  ),

  /**
   * sets Yaml value displayed to contents of detectionSettingsYaml or yamlAlertSettings
   * @method currentYamlAlert
   * @return {String}
   */
  currentYamlSettings: computed(
    'detectionSettingsYaml',
    function() {
      const detectionSettingsYaml = get(this, 'detectionSettingsYaml');
      return detectionSettingsYaml || get(this, 'yamlAlertSettings');
    }
  ),


  isErrorMsg: computed(
    'errorMsg',
    function() {
      const errorMsg = get(this, 'errorMsg');
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
      //set(this, 'subscriptionGroupNames', json.filterBy('yaml'));
      return json.filterBy('yaml');
    } catch (error) {
      notifications.error('Failed to retrieve subscription groups.', 'Error');
    }
  }).drop(),

  didRender(){
    this._super(...arguments);

    later(() => {
      this._buildSliderButton();
    });
  },

  // Helper function that builds the subchart region buttons
  _buildSliderButton() {
    const componentId = this.get('componentId');
    const resizeButtons = d3.select(`.${componentId}`).selectAll('.resize');

    resizeButtons.append('circle')
      .attr('cx', 0)
      .attr('cy', 30)
      .attr('r', 10)
      .attr('fill', '#0091CA');
    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 0)
      .attr("y1", 27)
      .attr("x2", 0)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", -5)
      .attr("y1", 27)
      .attr("x2", -5)
      .attr("y2", 33);

    resizeButtons.append('line')
      .attr('class', 'anomaly-graph__slider-line')
      .attr("x1", 5)
      .attr("y1", 27)
      .attr("x2", 5)
      .attr("y2", 33);
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(PREVIEW_DATE_FORMAT)}
    to ${moment(anomaly.endTime).format(PREVIEW_DATE_FORMAT)}`;
  },

  _filterAnomalies(rows) {
    return rows.filter(row => (row.startTime && row.endTime && !row.child));
  },

  _fetchTimeseries() {
    const metricUrn = get(this, 'metricUrn');
    const range = get(this, 'displayRange');
    const granularity = '15_MINUTES';
    const timezone = moment.tz.guess();

    set(this, 'errorTimeseries', null);

    const urlCurrent = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=current&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlCurrent)
      .then(checkStatus)
      .then(res => {
        set(this, 'timeseries', res);
        set(this, 'isLoading', false);
      });
    // .then(res => set(this, 'output', 'got timeseries'))
    // .catch(err => set(this, 'errorTimeseries', err));

    set(this, 'errorBaseline', null);

    const offset = get(this, 'compareMode');
    const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlBaseline)
      .then(checkStatus)
      .then(res => set(this, 'baseline', res));
    // .then(res => set(this, 'output', 'got baseline'))
    // .catch(err => set(this, 'errorBaseline', err));
  },

  _fetchAnomalies() {
    const analysisRange = get(this, 'analysisRange');
    const url = `/yaml/preview?start=${analysisRange[0]}&end=${analysisRange[1]}&tuningStart=0&tuningEnd=0`;

    set(this, 'errorAnomalies', null);

    const content = get(this, 'alertYaml');
    const postProps = {
      method: 'post',
      body: content,
      headers: { 'content-type': 'text/plain' }
    };

    fetch(url, postProps)
      .then(checkStatus)
      .then(res => {
        set(this, 'anomalies', this._filterAnomalies(res.anomalies));
        set(this, 'metricUrn', Object.keys(res.diagnostics['0'])[0]);
      })
      .then(() => {
        this._fetchTimeseries();
      })
    // .then(res => set(this, 'output', 'got anomalies'))
      .catch(err => {
        err.response.json()
          .then(res => {
            set(this, 'errorAnomalies', res.message);
            set(this, 'isLoading', false);
          });
      });
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
    * triggered by preview button
    */
    getPreview() {
      set(this, 'isLoading', true);
      set(this, 'showPreview', true);
      this._fetchAnomalies();
    },

    /**
     * resets given yaml field to default value for creation mode and server value for edit mode
     */
    resetYAML(field) {
      const isEditMode = get(this, 'isEditMode');
      if (field === 'anomaly') {
        if(isEditMode) {
          set(this, 'alertYaml', get(this, 'currentYamlAlertOriginal'));
        } else {
          const yamlAlertProps = get(this, 'yamlAlertProps');
          set(this, 'alertYaml', yamlAlertProps);
        }
      } else if (field === 'notification') {
        if(isEditMode) {
          set(this, 'detectionSettingsYaml', get(this, 'currentYamlSettingsOriginal'));
        } else {
          const yamlAlertSettings = get(this, 'yamlAlertSettings');
          set(this, 'detectionSettingsYaml', yamlAlertSettings);
        }
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
     * Activates 'Create changes' button and stores YAML content in alertYaml
     */
    onYMLSelectorAction(value) {
      set(this, 'disableYamlSave', false);
      set(this, 'alertYaml', value);
      set(this, 'errorMsg', '');
    },

    /**
     * Activates 'Create changes' button and stores YAML content in detectionSettingsYaml
     */
    onYMLSettingsSelectorAction(value) {
      set(this, 'disableYamlSave', false);
      set(this, 'detectionSettingsYaml', value);
    },

    /**
     * Fired by create button in YAML UI
     * Grabs YAML content and sends it
     */
    createAlertYamlAction() {
      const content = {
        detection: get(this, 'alertYaml'),
        notification: get(this, 'detectionSettingsYaml')
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
    },

    /**
     * Fired by save button in YAML UI
     * Grabs each yaml (alert and settings) and save them to their respective apis.
     */
    async saveEditYamlAction() {
      const {
        alertYaml,
        detectionSettingsYaml,
        notifications,
        alertId,
        subscriptionGroupId
      } = getProperties(this, 'alertYaml', 'detectionSettingsYaml', 'notifications', 'alertId', 'subscriptionGroupId');

      //PUT alert
      const alert_url = `/yaml/${alertId}`;
      const alertPostProps = {
        method: 'PUT',
        body: alertYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const alert_result = await fetch(alert_url, alertPostProps);
        const alert_status  = get(alert_result, 'status');
        const alert_json = await alert_result.json();
        if (alert_status !== 200) {
          set(this, 'errorMsg', get(alert_json, 'message'));
          notifications.error('Save alert yaml file failed.', 'Error');
        } else {
          notifications.success('Alert saved successfully', 'Done', alert_json);
        }
      } catch (error) {
        notifications.error('Save alert yaml file failed.', error);
      }

      //PUT settings
      const setting_url = `/yaml/notification/${subscriptionGroupId}`;
      const settingsPostProps = {
        method: 'PUT',
        body: detectionSettingsYaml,
        headers: { 'content-type': 'text/plain' }
      };
      try {
        const settings_result = await fetch(setting_url, settingsPostProps);
        const settings_status  = get(settings_result, 'status');
        const settings_json = await settings_result.json();
        if (settings_status !== 200) {
          set(this, 'errorMsg', get(settings_json, 'message'));
          notifications.error('Save settings yaml file failed.', 'Error');
        } else {
          notifications.success('Settings saved successfully', 'Done', settings_json);
        }
      } catch (error) {
        notifications.error('Save settings yaml file failed.', error);
      }
    }
  }
});
