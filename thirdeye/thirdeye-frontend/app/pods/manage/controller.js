/**
 * Handles logic for manage view
 * @module manage/controller
 * @exports manage
 */
import Controller from '@ember/controller';

export default Controller.extend({
  /**
   * Array of 'manage' sub navigation items
   */
  manageNav: [{
    className: 'menu-item--manage',
    link: 'manage.alerts',
    title: 'Alerts'
  }]
});
