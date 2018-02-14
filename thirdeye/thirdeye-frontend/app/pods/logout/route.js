import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

/**
 * Logout Page
 * This is necessary because we need to have access to this from
 * the old (non-Ember) UI
 */
export default Route.extend(AuthenticatedRouteMixin, {
  session: service(),

  routeIfAlreadyAuthenticated: 'rca',

  beforeModel() {
    this._super(...arguments);

    this.get('session').invalidate();
  }
});
