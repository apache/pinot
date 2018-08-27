import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import config from 'thirdeye-frontend/config/environment';

export default Route.extend(ApplicationRouteMixin, {
  moment: service(),
  session: service(),

  beforeModel() {
    // calling this._super to trigger ember-simple-auth's hook
    this._super(...arguments);
    // SM: leaving this here for reference if needed
    const isProdEnv = config.environment !== 'development';
    // invalidates session if cookie expired
    if (this.get('session.isAuthenticated')) {
      fetch('/auth')
        .then(checkStatus)
        .catch(() => {
          this.get('session').invalidate();
        });
    }

    this.get('moment').setTimeZone('America/Los_Angeles');
  },

  model(params, transition) {
    const { targetName } = transition;

    // This is used to hide the navbar when accessing the screenshot page
    return targetName !== 'screenshot';
  },

  /**
   * if the app is in test mode, have the sessions automatically authenticated
   * TODO: This is a hack. In future tests, this should be removed and use authenticateSession() instead
   * https://github.com/simplabs/ember-simple-auth/issues/727
   * @method afterModel
   * @return {undefined}
   */
  afterModel() {
    if (config.environment === 'test') {
      this.get('session.session').set('isAuthenticated', true);
    }
  },

  /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  sessionAuthenticated() {
    this._super(...arguments);
  },

  /**
   * Augments sessionInvalidated so that it doesn't redirect
   * to rootURL defined in environment.js
   * @override ApplicationRouteMixin.sessionInvalidated
   */
  sessionInvalidated() {
    this.transitionTo('login');
  },

  actions: {
    /**
     * @summary You can specify your own global default error handler by overriding the error handler on ApplicationRoute.
     * we will catch 401 and provide a specific 401 message on the login page. With that, we will also store the last transition attempt
     * to retry upon successful authentication. Last, we will logout the user to destroy the session and allow the user to login to be authenticated.
     */
    error: function(error, transition) {
      if (error.message === '401') {
        this.set('session.store.errorMsg', 'Your session expired. Please login again.');
      } else {
        this.set('session.store.errorMsg', 'Something went wrong. Please login again.');
      }
      this.transitionTo('logout');
    }
  }
});
