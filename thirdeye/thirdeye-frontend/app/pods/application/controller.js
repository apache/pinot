/**
 * Handles logic for app base view
 * @module  application
 * @exports application
 */
import { inject as service } from '@ember/service';

import { alias } from '@ember/object/computed';
import Controller from '@ember/controller';

export default Controller.extend({
  showNavbar: alias('model'),
  session: service(),

  /**
   * Global navbar items
   * @type {Array}
   */
  globalNavItems: [
    {
      className: 'dashboard',
      link: 'home',
      title: 'Home',
      isCustomLink: false
    },
    {
      className: 'anomalies',
      link: '/thirdeye#anomalies',
      isCustomLink: true,
      title: 'Anomalies'
    },
    {
      className: 'manage',
      link: 'manage.alerts',
      title: 'Alerts'
    },
    {
      className: 'rootcause',
      link: 'rootcause',
      title: 'Root Cause Analysis',
      isCustomLink: false
    }
  ]
});
