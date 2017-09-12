import Ember from 'ember';
import UnauthenticatedRouteMixin from 'ember-simple-auth/mixins/unauthenticated-route-mixin';

/**
 * Logout Page
 * This is necessary because we need to have access to this from
 * the old (non-Ember) UI
 */
export default Ember.Route.extend(UnauthenticatedRouteMixin, {
  session: Ember.inject.service(),

  routeIfAlreadyAuthenticated: 'rca',

  beforeModel() {
    this._super();

    this.get('session').invalidate();
  }
});
