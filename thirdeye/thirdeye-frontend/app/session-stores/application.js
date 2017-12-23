// app/session-stores/application.js
import CookieStore from 'ember-simple-auth/session-stores/cookie';

export default CookieStore.extend({
  restore() {
    return this._super();
  }
});
