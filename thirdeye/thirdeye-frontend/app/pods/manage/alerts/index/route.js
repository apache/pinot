import Ember from 'ember';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Route.extend({
  model() {
    return Ember.RSVP.hash({
      alerts: fetch('/thirdeye/entity/ANOMALY_FUNCTION').then(res => res.json()),
      subscriberGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      applications: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    })
  },

  setupController(controller, model, transition) {
    const { queryParams } = transition;
    const isSearchModeAll = !queryParams.selectedSearchMode
      || (queryParams.selectedSearchMode === 'All Alerts');
    const filterBlocks = {
      'Quick Filters': ['Alerts I subscribe to', 'Alerts I own', 'All alerts'],
      'Status': ['Active', 'Inactive'],
      'Subscription Groups': model.subscriberGroups.map(group => group.name),
      'Applications': model.applications.map(app => app.application),
      'Owner': model.alerts.map(alert => alert.createdBy)
    };

    // Dedupe and remove null or empty values
    for (var key in filterBlocks) {
      filterBlocks[key] = Array.from(new Set(filterBlocks[key].filter(value => Ember.isPresent(value))));
    }

    // Send filters to controller
    controller.setProperties({
      model,
      filterBlocks
    });

    // Pre-select all alerts if mode is right
    if (isSearchModeAll) {
      controller.setProperties({
        selectedAlerts: model.alerts,
        resultsActive: true
      });
    }
  }
});
