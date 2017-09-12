/**
 * Handles the 'manage' route.
 * @module manage/route
 * @exports manage route
 */
import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  /**
   * Default to 'alerts' for this model's root path request
   */
  afterModel: function(user, transition) {
    if (transition.targetName === this.routeName + '.index') {
      this.transitionTo('manage.alerts');
    }
  }
});
