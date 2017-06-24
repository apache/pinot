/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';
import fetch from 'fetch';

export default Ember.Controller.extend({
  /**
   * Array of metrics we're displaying
   */
  selectedMetric: {},
  selectedMetricDimensions: [],

  isMetricSelected: false,

  /**
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url)
      .then(res => res.json())
  }),

  /**
   * Handler for search by function name
   * Utilizing ember concurrency (task)
   */
  fetchMetricDimensions(metricId) {
    const url = 'data/autocomplete/dimensions/metric/' + metricId;
    return fetch(url)
      .then(res => res.json())
  },

  fetchAnomalyGraphData(metricId, dimension) {
    const url = '/timeseries/compare/' + metricId + '/1492564800000/1492593000000/1491355200000/1491383400000?dimension=' + dimension + '&granularity=MINUTES&filters={}';
    return fetch(url)
      .then(res => res.json())
  },

  triggerGraphLoad(metric, dimension) {
    let activeDimension = dimension || 'All';
    let metricId = metric.id || metric.metricId;
    console.log('got metric id here: ', metricId);
    this.fetchAnomalyGraphData(metricId, activeDimension).then(metricData => {
      this.set('isMetricSelected', true);
      this.set('selectedMetric', metricData);
      if (!dimension) {
        this.fetchMetricDimensions(metricId).then(dimensionData => {
          this.set('selectedMetricDimensions', dimensionData);
        });
      }
    });
  },

  /**
   * Placeholder for patterns of interest options
   */
  patternsOfInterest: ['Pattern One', 'Pattern Two'],
  selectedPattern: 'Pattern One',
  /**
   * Placeholder for dimensions options
   */
  dimensionExploration: ['First', 'Second', 'Third'],
  selectedDimension: 'Select One',
  /**
   * Placeholder for alert groups options
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allAlertsConfigGroups'),
  selectedGroup: 'Select an alert group, or create new',
  /**
   * Actions for create alert form view
   */
  actions: {
    /**
     * Function called when the dropdown value is updated
     * @method onChangeDropdown
     * @param {Object} selectedObj - If has dataset, this is the selected value from dropdown
     * @return {undefined}
     */
    onChangeDropdown(selectedObj) {
      if (selectedObj) {
        this.set('selectorVal', selectedObj);
        this.triggerGraphLoad(selectedObj, null);
      } else {
        this.set('selectorVal', '');
      }
    },

    onChangeDimension(selectedObj) {
      let currMetric = this.get('selectedMetric');
      console.log('currMetric: ', currMetric.metricId);
      if (selectedObj) {
        this.set('dimensionSelectorVal', selectedObj);
        this.triggerGraphLoad(currMetric, selectedObj);
      } else {
        this.set('dimensionSelectorVal', '');
      }
    },

    submit(data) {
      console.log(data);
    }
  }
});
