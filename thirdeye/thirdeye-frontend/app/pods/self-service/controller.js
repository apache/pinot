/**
 * Handles logic for self-service view
 * @module  self-service
 * @exports self-service
 */
import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Array of 'self-service' sub navigation items
   */
  selfServiceNav: [
    {
      className: 'menu-item--manage',
      link: 'self-service.manage',
      title: 'Manage Alerts'
    },
    {
      className: 'menu-item--create',
      link: 'self-service.create',
      title: 'Manage Alert Subscription'
    },
    {
      className: 'menu-item--onboard',
      link: 'self-service.onboard',
      title: 'Onboard Metrics'
    }
  ]
});
