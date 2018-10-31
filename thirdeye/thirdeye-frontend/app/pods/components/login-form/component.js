import Component from '@ember/component';

export default Component.extend({
  classNames: ['nacho-login-form'],
  username: null,
  password: null,
  actions: {
    /**
     * Handles the submit button
     */
    onLogin() {
      const credentials = {
        principal: this.get('username'),
        password: this.get('password')
      };
      this.attrs.onLogin(credentials);
    }
  }
});
