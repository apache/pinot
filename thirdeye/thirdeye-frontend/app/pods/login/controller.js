import { inject as service } from '@ember/service';
import Controller from '@ember/controller';
import { get } from '@ember/object';
import _ from 'lodash';

export default Controller.extend({
  errorMessage: null,
  session: service(),


  _makeTransition(fromUrl, session) {
    const sessionFromUrl = get(session, 'store.fromUrl');
    const lastIntentTransition = sessionFromUrl.lastIntentTransition.intent;
    const name = lastIntentTransition.name;

    if (fromUrl) {
      this.transitionToRoute(fromUrl);
    } else if (lastIntentTransition && name) {
      const queryParams = lastIntentTransition.queryParams;
      const contexts = lastIntentTransition.contexts;
      if (contexts && contexts[0] && !_.isEmpty(queryParams)) {
        this.transitionToRoute(name, contexts[0], { queryParams });
      } else if (contexts && contexts[0] && _.isEmpty(queryParams)) {
        this.transitionToRoute(name, contexts[0]);
      } else if (contexts && !_.isEmpty(queryParams)) {
        this.transitionToRoute(name, { queryParams });
      } else {
        this.transitionToRoute(name);
      }
      //TODO: The transition retry the last transtion prior to session expired isn't ideal here. Will save to figure out why - lohuynh
      //transition.retry();
    }
  },

  actions: {
    /**
     * Calls the application's authenticate method
     * @param {Object} credentials containins username and password
     * @returns {undefiend}
     */
    onLogin(credentials) {
      const fromUrl = this.get('fromUrl');
      const session = get(this, 'session');
      const errorMsg = get(session, 'store.errorMsg');

      // reset the messages locally and in the session's store
      this.set('errorMessage', null);
      this.set('session.store.errorMsg', null);

      session.authenticate('authenticator:custom-ldap', credentials)
        .then(() => {
          this._makeTransition(fromUrl, session);
        })
        .catch(({ responseText = 'Bad Credentials' }) => {
          if (errorMsg) {
            this.set('errorMessage', errorMsg);
          } else {
            this.set('errorMessage', responseText);
          }
        });
    }
  }
});
