/**
 * Component for displaying error info in modal
 * @module components/modals/error-modal
 * @property {String} errorInfo - error info to display in body
 * @example
   {{modals/error-modal
     errorInfo="Something"
   }}
 * @exports error-modal
 * @author hjackson
 */

import Component from '@ember/component';
import config from 'thirdeye-frontend/config/environment';

export default Component.extend({
  email: config.email
});
