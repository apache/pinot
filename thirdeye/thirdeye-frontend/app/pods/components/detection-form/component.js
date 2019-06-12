/**
 * Component to render the detection configuration form editor.
 * @module components/detection-form
 * @property {boolean} isAlertNameDuplicate - triggered by alert name validation action on parent
 * @property {boolean} isAlertNameUserModified - triggered by alert name validation action on parent
 * @property {function} validateAlertName - action passed in from parent, shared with subscrtiption-form component
 * @example
   {{detection-form
     validateAlertName=(action "someAction")
     isAlertNameUserModified=isAlertNameUserModified
     isAlertNameDuplicate=isAlertNameDuplicate
   }}
 * @author hjackson
 */

import Component from '@ember/component';
import {computed, getWithDefault} from '@ember/object';
import {checkStatus} from 'thirdeye-frontend/utils/utils';
import RSVP from "rsvp";
import fetch from 'fetch';
import {
  selfServeApiGraph, selfServeApiCommon
} from 'thirdeye-frontend/utils/api/self-serve';
import config from 'thirdeye-frontend/config/environment';
import { task, timeout } from 'ember-concurrency';

export default Component.extend({
  alertFunctionName: null,
  /**
   * Properties we expect to receive for the detection-form
   */
  metricHelpMailto: `mailto:${config.email}?subject=Metric Onboarding Request (non-additive UMP or derived)`,
  selectedMetricOption: null,
  isEditMode: false,
  metricLookupCache: [],
  isMetricSelected: false,
  isMetricDataInvalid: false,
  validateAlertName: null, // action passed in by parent
  selectedDimension: null,
  selectedGranularity: null,
  selectedApplication: null,

  /**
   * Determines whether input fields in general are enabled. When metric data is 'invalid',
   * we will still enable alert creation.
   * @method generalFieldsEnabled
   * @return {Boolean}
   */
  generalFieldsEnabled: computed.or('isMetricSelected', 'isMetricDataInvalid'),

  /**
   * Generate alert name primer based on user selections
   * @type {String}
   */
  functionNamePrimer: computed(
    'selectedDimension',
    'selectedGranularity',
    'selectedApplication',
    'selectedMetricOption',
    function() {
      const {
        selectedDimension,
        selectedGranularity,
        selectedApplication,
        selectedMetricOption
      } = this.getProperties(
        'selectedDimension',
        'selectedGranularity',
        'selectedApplication',
        'selectedMetricOption'
      );
      const dimension = selectedDimension ? `${selectedDimension.camelize()}_` : '';
      const granularity = selectedGranularity ? selectedGranularity.toLowerCase().camelize() : '';
      const app = selectedApplication ? `${selectedApplication.camelize()}_` : 'applicationName_';
      const metric = selectedMetricOption ? `${selectedMetricOption.name.camelize()}_` : 'metricName_';
      return `${app}${metric}${dimension}${granularity}`;
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
      granularities: fetch(selfServeApiGraph.metricGranularity(metricId)).then(res => checkStatus(res, 'get', true)),
      filters: fetch(selfServeApiGraph.metricFilters(metricId)).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(selfServeApiGraph.metricDimensions(metricId)).then(res => checkStatus(res, 'get', true))
    };
    return RSVP.hash(promiseHash);
  },

  /**
   * Auto-generate the alert name until the user directly edits it
   * @method _modifyAlertFunctionName
   * @return {undefined}
   */
  _modifyAlertFunctionName() {
    const {
      functionNamePrimer,
      isAlertNameUserModified
    } = this.getProperties('functionNamePrimer', 'isAlertNameUserModified');
    // If user has not yet edited the alert name, continue to auto-generate it.
    if (!isAlertNameUserModified) {
      this.set('alertFunctionName', functionNamePrimer);
    }
    // Each time we modify the name, we validate it as well to ensure no duplicates exist.
    this.get('validateAlertName')(this.get('alertFunctionName'));
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
        topDimensions: [],
        isMetricDataLoading: true,
        selectedMetricOption: selectedObj,
        isMetricSelected: true
      });
      this._fetchMetricData(selectedObj.id)
        .then((metricHash) => {
          const { maxTime, filters, dimensions, granularities } = metricHash;
          const targetMetric = this.get('metricLookupCache').find(metric => metric.id === selectedObj.id);
          const inGraphLink = getWithDefault(targetMetric, 'extSourceLinkInfo.INGRAPH', null);
          // In the event that we have an "ingraph" metric, enable only "minute" level granularity
          const adjustedGranularity = (targetMetric && inGraphLink) ? granularities.filter(g => g.toLowerCase().includes('minute')) : granularities;

          this.setProperties({
            maxTime,
            filters,
            dimensions,
            metricLookupCache: [],
            granularities: adjustedGranularity,
            originalDimensions: dimensions,
            metricGranularityOptions: adjustedGranularity,
            selectedGranularity: adjustedGranularity[0],
            alertFunctionName: this.get('functionNamePrimer')
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
     * Bubble up to validateAlertName action
     * @method validateName
     * @param {String} userProvidedName - The new alert name
     * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
     * @return {undefined}
     */
    validateName(userProvidedName, userModified = false) {
      this.get('validateAlertName')(userProvidedName, userModified);
    },

    /**
     * Set our selected granularity. Trigger graph reload.
     * @method onSelectGranularity
     * @param {Object} selectedObj - The selected granularity option
     * @return {undefined}
     */
    onSelectGranularity(selectedObj) {
      this.setProperties({
        selectedGranularity: selectedObj,
        isSecondaryDataLoading: true
      });
      this._modifyAlertFunctionName();
    },

    /**
     * When a dimension is selected, fetch new anomaly graph data based on that dimension
     * and trigger a new graph load, showing the top contributing subdimensions.
     * @method onSelectFilter
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
        this._modifyAlertFunctionName();
      }
    },

    /**
     * When a filter is selected, fetch new anomaly graph data based on that filter
     * and trigger a new graph load. Also filter dimension names already selected as filters.
     * @method onSelectFilter
     * @param {Object} selectedFilters - The selected filters to apply
     * @return {undefined}
     */
    onSelectFilter(selectedFilters) {
      const selectedFilterObj = JSON.parse(selectedFilters);
      const dimensionNameSet = new Set(this.get('originalDimensions'));
      const filterNames = Object.keys(JSON.parse(selectedFilters));
      let isSelectedDimensionEqualToSelectedFilter = false;

      // Remove selected filters from dimension options only if filter has single entity
      for (var key of filterNames) {
        if (selectedFilterObj[key].length === 1) {
          dimensionNameSet.delete(key);
          if (key === this.get('selectedDimension')) {
            isSelectedDimensionEqualToSelectedFilter = true;
          }
        }
      }
      // Update dimension options and loader
      this.setProperties({
        dimensions: [...dimensionNameSet],
        isSecondaryDataLoading: true
      });
      // Do not allow selected dimension to match selected filter
      if (isSelectedDimensionEqualToSelectedFilter) {
        this.set('selectedDimension', 'All');
      }
    }
  }
});
