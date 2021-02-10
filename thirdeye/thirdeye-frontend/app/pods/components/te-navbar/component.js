/**
 * Handles the nav bar component logic
 * @module  components/te-navbar
 * @exports anomaly-navbar
 */
import Component from '@ember/component';
import config from '../../../config/environment';
import { reads } from '@ember/object/computed';
import { inject as service } from '@ember/service';

export default Component.extend({
  /**
   * Component's tag name
   */
  tagName: 'nav',

  authService: service('session'),
  username: reads('authService.data.authenticated.name'),

  /**
   * Apply property-based class namete
   */
  classNameBindings: ['navClass'],

  /**
   * List of associated classes
   */
  classNames: ['te-nav'],

  /**
   * App name from environment settings (string)
   */
  webappName: config.appName,

  /**
   * Expanded flag for the help icon
   * @type {boolean}
   */
  isExpanded: false,

  actions: {
    /**
     * Toggles the isExpanded property on click
     */
    onToggleExpanded() {
      this.toggleProperty('isExpanded');
    }
  }
});
