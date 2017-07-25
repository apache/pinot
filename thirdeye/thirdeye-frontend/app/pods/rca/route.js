import Ember from 'ember';
import fetch from 'fetch';

export default Ember.Route.extend({

  model(params, transition) {
    const {
      'rca.details': detailsParams = {}
    } = transition.params;

    const { metricId = null } = detailsParams;
    if (!metricId) { return; }

    return fetch(`/data/metric/${metricId}`)
      .then(res => res.json());
  },

  actions: {
    transitionToDetails(id) {
      this.transitionTo('rca.details', id, { queryParams: {
        startDate: undefined,
        endDate: undefined,
        analysisStart: undefined,
        analysisEnd: undefined,
        granularity: undefined,
        filters: JSON.stringify({}),
        compareMode: 'WoW'
      }});
    }
  }
});
