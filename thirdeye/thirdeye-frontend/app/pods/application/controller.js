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
      link: 'index',
      title: 'Home'
    },
    {
      className: 'anomalies',
      link: '/1426/thirdeye',
      isCustomLink: true,
      title: 'Anomalies'
    },
    {
      className: 'rca',
      link: 'example',
      id: '1',
      title: 'Root Cause Analysis'
    },
    {
      className: 'alert',
      link: 'self-service',
      title: 'Self-Service'
    }
  ]

});
