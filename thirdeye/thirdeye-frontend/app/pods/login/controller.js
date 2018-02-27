import { inject as service } from '@ember/service';
import Controller from '@ember/controller';

export default Controller.extend({
  errorMessage: null,
  session: service(),
  queryParams: ['fromUrl'],
  fromUrl: null,

  /**
   * Redirects to the passed url
   * @param {String} url
   * @return {undefined}
   */
  redirect(url) {
    window.location.replace(url);
  },

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
            this.redirect(fromUrl);
          }
        })
        .catch(({ responseText = 'Bad Credentials' }) => {
          this.set('errorMessage', responseText);
        });
    }
  }
});
