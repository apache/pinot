import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import config from 'thirdeye-frontend/config/environment';

export default Route.extend(ApplicationRouteMixin, {
  moment: service(),
  session: service(),
  notifications: service('toast'),

  beforeModel() {
    // calling this._super to trigger ember-simple-auth's hook
    this._super(...arguments);

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
    error: function(error, /*transition*/) {
      const isDevEnv = config.environment === 'development';

      const notifications = this.get('notifications');
      const toastOptions = {
        timeOut: '4000',
        positionClass: 'toast-bottom-right'
      };
      const toastMsg = 'Something went wrong with a request. Please try again or come back later.';
      if (error.message === '401' || (error.response && error.response.status === '401')) {
        this.set('session.store.errorMsg', 'Your session expired. Please login again.');
        this.transitionTo('logout');
      } else if (error.message === '500' || (error.response && error.response.status === '500')) {
        notifications.error(toastMsg, '500 error detected', toastOptions);
      } else {
        const errorStatus = error.response ? error.response.status : 'Unknown';
        notifications.error(toastMsg, `${errorStatus} error detected`, toastOptions);
      }
      if (isDevEnv) {
        throw error;
      }
    }
  }
});
