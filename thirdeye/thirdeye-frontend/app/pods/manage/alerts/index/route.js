import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import fetch from 'fetch';
import { get, getWithDefault } from '@ember/object';
import { inject as service } from '@ember/service';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import {
  enrichAlertResponseObject,
  populateFiltersLocal
} from 'thirdeye-frontend/utils/yaml-tools';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { yamlAPI } from 'thirdeye-frontend/utils/api/self-serve';
import { entityMappingApi } from 'thirdeye-frontend/utils/api/entity-mapping';
import config from 'thirdeye-frontend/config/environment';

export default Route.extend(AuthenticatedRouteMixin, {

  // Make duration service accessible
  durationCache: service('services/duration'),
  session: service(),

  model() {
    return hash({
      polishedDetectionYaml: fetch(yamlAPI.getPaginatedAlertsUrl()).then(checkStatus),
      rules: fetch(entityMappingApi.getRulesUrl)
        .then(checkStatus)
        .then(rules => rules.map(r => r.type))
    });
  },

  afterModel(model) {
    this._super(model);

    const alertsFromResponse = (model.polishedDetectionYaml || {}).elements;

    const alerts = enrichAlertResponseObject(alertsFromResponse);

    let user = getWithDefault(get(this, 'session'), 'data.authenticated.name', null);
    let token = config.userNameSplitToken;

    user = user ? user.split(token)[0] : user;
    // Add these filtered arrays to the model (they are only assigned once)

    Object.assign(model, { alerts, user });
  },

  setupController(controller, model) {

    const filterBlocksLocal = populateFiltersLocal(model.alerts, model.rules);

    // Send filters to controller
    controller.setProperties({
      model,
      resultsActive: true,
      filteredAlerts: model.polishedDetectionYaml.elements,
      filterBlocksLocal,
      paramsForAlerts: {},
      sortModes: ['Edited:first', 'Edited:last', 'A to Z', 'Z to A'], // Alerts Search Mode options
      originalAlerts: model.polishedDetectionYaml.elements,
      totalNumberOfAlerts: model.polishedDetectionYaml.count
    });
  },

  actions: {
    /**
     * Clear duration cache (time range is reset to default when entering new alert page from index)
     * @method willTransition
     */
    willTransition(transition) {
      this.get('durationCache').resetDuration();
      this.controller.set('isLoading', true);

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

    /**
     * Once transition is complete, remove loader
     */
    didTransition() {
      this.controller.set('isLoading', false);
    },

    /**
    * Refresh route's model.
    * @method refreshModel
    */
    refreshModel() {
      this.refresh();
    }
  }
});
