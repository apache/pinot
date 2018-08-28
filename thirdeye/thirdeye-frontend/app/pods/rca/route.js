import Route from '@ember/routing/route';
import fetch from 'fetch';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { inject as service } from '@ember/service';
import { checkStatus } from 'thirdeye-frontend/utils/utils';

export default Route.extend(AuthenticatedRouteMixin, {
  session: service(),

  model(params, transition) {
    const {
      'rca.details': detailsParams = {}
    } = transition.params;

    const { metric_id = null } = detailsParams;

    if (!metric_id) { return {}; }

    return fetch(`/data/metric/${metric_id}`)
      .then(checkStatus);
  },

  setupController(controller, model) {
    this._super(...arguments);

    // clears the controller's primaryMetric
    if (!Object.keys(model).length) {
      controller.set('primaryMetric', null);
    }
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    },
    error() {
      // The `error` hook is also provided the failed
      // `transition`, which can be stored and later
      // `.retry()`d if desired.
      return true;
    },

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
