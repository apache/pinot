import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import fetch from 'fetch';
import { inject as service } from '@ember/service';
import { checkStatus } from 'thirdeye-frontend/utils/utils';

export default Route.extend({

  // Make duration service accessible
  durationCache: service('services/duration'),
  session: service(),
  model() {
    return hash({
      alerts: fetch('/thirdeye/entity/ANOMALY_FUNCTION').then(checkStatus),
      subscriberGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(checkStatus),
      applications: fetch('/thirdeye/entity/APPLICATION').then(checkStatus)
    });
  },

  setupController(controller, model, transition) {
    const { queryParams } = transition;
    const isSearchModeAll = !queryParams.selectedSearchMode
      || (queryParams.selectedSearchMode === 'All Alerts');
    const filterBlocks = [
      {
        name: 'Quick Filters',
        filterKeys: ['Alerts I subscribe to', 'Alerts I own', 'All alerts']
      },
      {
        name: 'Status',
        filterKeys: ['Active', 'Inactive']
      },
      {
        name: 'Subscription Groups',
        filterKeys: model.subscriberGroups.map(group => group.name)
      },
      {
        name: 'Applications',
        filterKeys: model.applications.map(app => app.application)
      },
      {
        name: 'Owner',
        filterKeys: model.alerts.map(alert => alert.createdBy)
      }
    ];

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
