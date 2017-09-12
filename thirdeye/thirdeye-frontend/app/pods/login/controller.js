import Ember from 'ember';

export default Ember.Controller.extend({
  errorMessage: null,
  session: Ember.inject.service(),
  actions: {

    /**
     * Calls the application's authenticate method
     * @param {Object} credentials containins username and password
     * @returns {undefiend}
     */
    onLogin(credentials) {
      this.set('errorMessage', null);

      this.get('session')
        .authenticate('authenticator:custom-ldap', credentials)
        .catch(({ responseText = 'Bad Credentials' }) => {
          this.set('errorMessage', responseText);
        });
    }
  }
});
