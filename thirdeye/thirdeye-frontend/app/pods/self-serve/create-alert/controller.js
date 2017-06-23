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
  selectedMetrics: [],

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

  triggerGraphLoad(metricId) {
    console.log('got metric id here: ', metricId);
    console.log('fetching data : ', this.fetchAnomalyGraphData(metricId));
  },

  fetchAnomalyGraphData(metricId) {
    const url = '/timeseries/compare/${metricId}/1497596399999/1498147199999/1496991599999/1497542399999?dimension=All&granularity=HOURS&filters={}';
    return fetch(url)
      .then(res => res.json())
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
        this.triggerGraphLoad(selectedObj);
      } else {
        this.set('selectorVal', '');
      }
    },

    submit(data) {
      console.log(data);
    }
  }
});
