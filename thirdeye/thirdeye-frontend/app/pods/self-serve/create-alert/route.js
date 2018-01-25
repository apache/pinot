/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import Ember from 'ember';
import fetch from 'fetch';
import RSVP from 'rsvp';
import { postProps, checkStatus } from 'thirdeye-frontend/utils/utils';

export default Ember.Route.extend({
  queryParams: {
    newUx: {
      refreshModel: false,
      replace: true
    }
  },

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model(params, transition) {
    return RSVP.hash({
      // Fetch all alert group configurations
      isNewUx: transition.queryParams.newUx || false,
      allConfigGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      allAppNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    });
  },

  /**
   * Model hook for the create alert route.
   * @method resetController
   * @param {Object} controller - active controller
   * @param {Boolean} isExiting - exit status
   * @param {Object} transition - transition obj
   * @return {undefined}
   */
  resetController(controller, isExiting) {
    this._super(...arguments);
    if (isExiting) {
      controller.clearAll();
    }
  },

  actions: {
    /**
    * Refresh route's model.
    * @method refreshModel
    */
    refreshModel() {
      this.refresh();
    },

    /**
    * Trigger alert replay sequence after create has finished
    * @param {Number} alertId - Id of newly created alert function
    * @method triggerReplaySequence
    */
    triggerReplaySequence(alertId) {
      const replayUrl = this.controller.buildReplayUrl(alertId);
      fetch(replayUrl, postProps('')).then(checkStatus)
        .then((replayId) => {
          this.transitionTo('manage.alert', alertId, { queryParams: { replayId }});
        })
        // In the event of failure to trigger, transition anyway, without replay Id
        // The new API will handle notifying users of job failures.
        .catch(() => {
          this.transitionTo('manage.alert', alertId, { queryParams: { replayId: 0 }});
        });
    }
  },

});
