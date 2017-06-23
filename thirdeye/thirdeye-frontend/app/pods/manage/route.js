/**
 * Handles the 'manage' route.
 * @module manage/route
 * @exports manage route
 */
import Ember from 'ember';

export default Ember.Route.extend({
  /**
   * Default to 'alerts' for this model's root path request
   */
  afterModel: function(user, transition) {
    if (transition.targetName === this.routeName + '.index') {
      this.transitionTo('manage.alerts');
    }
  }
});
