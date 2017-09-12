import Base from 'ember-simple-auth/authenticators/base';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Base.extend({
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

    return fetch(url, postProps).then(checkStatus);
  },

  restore() {
    return Promise.resolve();
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
