import Ember from 'ember';
import fetch from 'fetch';

export default Ember.Route.extend({
  model() {
    const url = '/thirdeye/entity/ANOMALY_FUNCTION';
    return fetch(url).then(res => res.json());
  },

  setupController(controller, model, transition) {
    const { queryParams } = transition;
    const isSearchModeAll = !queryParams.selectedSearchMode
      || (queryParams.selectedSearchMode === 'All');

    controller.set('model', model); 

    if (isSearchModeAll) {
      controller.setProperties({
        selectedAlerts: model,
        resultsActive: true
      });
    }
  }
});


