import Ember from 'ember';
import fetch from 'fetch';

export default Ember.Route.extend({
  model() {
    // const url = '/thirdeye/entity/ANOMALY_FUNCTION';
    return Ember.RSVP.hash({
      filters: fetch('/thirdeye/entity/ANOMALY_FUNCTION').then(res => res.json()),
      suscriberGroupNames: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      applicationNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    })
  },

  setupController(controller, model, transition) {
    const { queryParams } = transition;
    const isSearchModeAll = !queryParams.selectedSearchMode
      || (queryParams.selectedSearchMode === 'All Alerts');

    controller.set('model', model);

    if (isSearchModeAll) {
      controller.setProperties({
        selectedAlerts: model.filters,
        resultsActive: true
      });
    }
  }
});
