import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

/**
 * Logout Page
 * This is necessary because we need to have access to this from
 * the old (non-Ember) UI
 */
export default Ember.Route.extend(AuthenticatedRouteMixin, {
  session: Ember.inject.service(),

  routeIfAlreadyAuthenticated: 'rca',

  beforeModel() {
    this._super(...arguments);

    this.get('session').invalidate();
  }
});
