import Ember from 'ember';
import fetch from 'fetch';
import { toBaselineUrn, toCurrentUrn } from 'thirdeye-frontend/utils/rca-utils';
import { task, timeout } from 'ember-concurrency';
import _ from 'lodash';

export default Ember.Component.extend({
  classNames: ['rootcause-select-metric-dimension'],

  selectedUrn: null, // ""

  onSelection: null, // function (metricUrn)

  placeholder: 'Search for a Metric',

  //
  // internal
  //
  mostRecentSearch: null, // promise

  selectedUrnCache: null, // ""

  selectedMetric: null, // {}

  /**
   * Ember concurrency task that triggers the metric autocomplete
   */
  searchMetrics: task(function* (metric) {
    yield timeout(1000);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url)
      .then(res => res.json());
  }),

  didReceiveAttrs() {
    this._super(...arguments);

    const { selectedUrn, selectedUrnCache } = this.getProperties('selectedUrn', 'selectedUrnCache');

    if (!_.isEqual(selectedUrn, selectedUrnCache)) {
      this.set('selectedUrnCache', selectedUrn);

      if (selectedUrn) {
        const url = `/data/metric/${selectedUrn.split(':')[2]}`;
        fetch(url)
          .then(res => res.json())
          .then(res => this.set('selectedMetric', res));
      } else {
        this.set('selectedMetric', null);
      }
    }
  },

  actions: {
    /**
     * Action handler for metric search changes
     * @param {Object} metric
     */
    onChange(metric) {
      const { onSelection } = this.getProperties('onSelection');
      if (!onSelection) { return; }

      const { id } = metric;
      if (!id) { return; }

      const metricUrn = `thirdeye:metric:${id}`;
      const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };

      onSelection(updates);
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
