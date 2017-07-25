import Ember from 'ember';
import { task, timeout } from 'ember-concurrency';

export default Ember.Controller.extend({
  primaryMetric: Ember.computed.oneWay('model'),
  mostRecentSearch: null,

  /**
   * Ember concurrency task that triggers the metric autocomplete
   */
  searchMetrics: task(function* (metric) {
    yield timeout(600);
    let url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url)
      .then(res => res.json());
  }),

  placeholder: Ember.computed(function() {
    'Search for a Metric';
  }),

  actions: {
    /**
     * Action handler for metric search changes
     * @param {Object} metric
     */
    onMetricChange(metric) {
      const { id } = metric;
      if (!id) { return; }
      this.set('primaryMetric', metric);

      this.send('transitionToDetails', id);
    },

    /**
     * Performs a search task while cancelling the previous one
     * @param {Array} metrics
     */
    onSearch(metrics) {
      const lastSearch = this.get('mostRecentSearch');
      if (lastSearch) {
        lastSearch.cancel();
      }
      const task = this.get('searchMetrics');
      const taskInstance = task.perform(metrics);
      this.set('mostRecentSearch', taskInstance);
      return taskInstance;
    }
  }
});
