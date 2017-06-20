/**
 * Handles logic for app base view
 * @module  application
 * @exports application
 */
import Ember from 'ember';

export default Ember.Controller.extend({

  /**
   * Global navbar items
   * @type {Array}
   */
  globalNavItems: [
    {
      className: 'home',
      link: '/thirdeye',
      isCustomLink: true,
      title: 'Home'
    },
    {
      className: 'anomalies',
      link: '/thirdeye#anomalies',
      isCustomLink: true,
      title: 'Anomalies'
    },
    {
      className: 'rca',
      link: '/thirdeye#analysis',
      isCustomLink: true,
      title: 'Root Cause Analysis'
    },
    {
      className: 'alert',
      link: 'self-service',
      title: 'Self-Service'
      // hidden: true
    }
  ]

});
