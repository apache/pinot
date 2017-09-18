import Ember from 'ember';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';

export default Ember.Route.extend(ApplicationRouteMixin, {
  moment: Ember.inject.service(),
  session: Ember.inject.service(),

  beforeModel() {
    // calling this._super to trigger ember-simple-auth's hook
    this._super(...arguments);

    this.get('moment').setTimeZone('America/Los_Angeles');
  },

  model(params, transition) {
    const { targetName } = transition;

    // This is used to hide the navbar when accessing the screenshot page
    return targetName !== 'screenshot';
  },
  /**
   * Redirect route after authentication
   * @override ApplicationRouteMixin.routeAfterAuthentication
   */
  routeAfterAuthentication: 'rca',
    /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  sessionAuthenticated() {
    this._super(...arguments);

    this.transitionTo('rca');
  }
});
