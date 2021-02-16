import Component from '@ember/component';

export default Component.extend({
  classNames: ['nacho-login-form'],
  username: null,
  password: null,
  /**
   * @public
   * Controller injects method for processing login
   */
  onLogin: () => {},
  actions: {
    /**
     * Handles the submit button
     */
    _onLogin() {
      const credentials = {
        principal: this.get('username'),
        password: this.get('password')
      };
      this.onLogin(credentials);
    }
  }
});
