import { inject as service } from '@ember/service';
import Base from 'ember-simple-auth/authenticators/base';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import RSVP from 'rsvp';

export default Base.extend({
  session: service(),
  /**
   * Implements Base authenticator's authenticate method.
   * @param {Object}  credentials containing username and password
   * @return {Promise}
   */
  authenticate(credentials) {
    const url = '/auth/authenticate';
    const postProps = {
      method: 'post',
      body: JSON.stringify(credentials),
      headers: { 'content-type': 'Application/Json' }
    };

    return fetch(url, postProps).then(checkStatus).then((res) => {
      // set expiration to 7 days
      let expiration = 60 * 60 * 24 * 7;

      this.set('session.store.cookieExpirationTime', expiration);

      return res;
    });
  },

  restore(data) {
    return RSVP.resolve(data);
  },

  /**
   * Implements Base authenticator's invalidate method.
   * @return {Promise}
   */
  invalidate() {
    const url = '/auth/logout';
    return fetch(url)
      .then(() => {
        return this._super();
      });
  }
});
