/**
 * Handles the nav bar component logic
 * @module  components/te-navbar
 * @exports anomaly-navbar
 */
import Ember from 'ember';
import config from '../../../config/environment';

export default Ember.Component.extend({

  /**
   * Component's tag name
   */
  tagName: 'nav',

  /**
   * Apply property-based class name
   */
  classNameBindings: ['navClass'],

  /**
   * List of associated classes
   */
  classNames: ['te-nav'],

  /**
   * App name from environment settings (string)
   */
  webappName: config.appName
});
