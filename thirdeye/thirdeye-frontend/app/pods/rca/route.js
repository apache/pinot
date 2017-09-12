import Ember from 'ember';
import fetch from 'fetch';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {

  model(params, transition) {
    const {
      'rca.details': detailsParams = {}
    } = transition.params;

    const { metricId = null } = detailsParams;

    if (!metricId) { return {}; }

    return fetch(`/data/metric/${metricId}`)
      .then(res => res.json());
  },

  setupController(controller, model) {
    this._super(...arguments);

    // clears the controller's primaryMetric
    if (!Object.keys(model).length) {
      controller.set('primaryMetric', null);
    }
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
