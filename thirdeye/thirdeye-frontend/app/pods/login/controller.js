import { inject as service } from '@ember/service';
import Controller from '@ember/controller';

export default Controller.extend({
  errorMessage: null,
  session: service(),

  actions: {
    /**
     * Calls the application's authenticate method
     * @param {Object} credentials containins username and password
     * @returns {undefiend}
     */
    onLogin(credentials) {
      const fromUrl = this.get('fromUrl');
      this.set('errorMessage', null);

      this.get('session')
        .authenticate('authenticator:custom-ldap', credentials)
        .then(() => {
          if (fromUrl) {
            this.transitionToRoute(fromUrl);
          }
        })
        .catch(({ responseText = 'Bad Credentials' }) => {
          this.set('errorMessage', responseText);
        });
    }
  }
});
