import Ember from 'ember';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Route.extend({
  model() {
    return Ember.RSVP.hash({
      alerts: fetch('/thirdeye/entity/ANOMALY_FUNCTION').then(res => res.json()),
      suscriberGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      applications: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    })
  },

  setupController(controller, model, transition) {
    const { queryParams } = transition;
    const isSearchModeAll = !queryParams.selectedSearchMode
      || (queryParams.selectedSearchMode === 'All Alerts');
    let groupFunctionIds = [];
    let foundAlert = {};

    // Itereate through config groups to tag included alerts with app name
    for (let config of model.suscriberGroups) {
      groupFunctionIds = config.emailConfig && config.emailConfig.functionIds ? config.emailConfig.functionIds : [];
      for (let id of groupFunctionIds) {
        foundAlert = _.find(model.alerts, function(alert) {
          return alert.id === id;
        });
        if (foundAlert) {
          foundAlert.application = config.application;
        }
      }
    }

    controller.set('model', model);

    if (isSearchModeAll) {
      controller.setProperties({
        selectedAlerts: model.alerts,
        resultsActive: true
      });
    }
  }
});
