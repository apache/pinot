import Ember from 'ember';
import UnauthenticatedRouteMixin from 'ember-simple-auth/mixins/unauthenticated-route-mixin';

export default Ember.Route.extend(UnauthenticatedRouteMixin, {
  session: Ember.inject.service(),
  errorMessage: null,

  /**
   * The route to redirect to if already logged in
   * @override UnauthenticatedRouteMixin.routeIfAlreadyAuthenticated
   */
  routeIfAlreadyAuthenticated: 'rca'
});
