/**
 * Handles logic for manage view
 * @module manage/controller
 * @exports manage
 */
import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Array of 'manage' sub navigation items
   */
  manageNav: [{
    className: 'menu-item--manage',
    link: 'manage.alerts',
    title: 'Alerts'
  }]
});
