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
  webappName: config.appName,

  /**
   * Boolean to distinguish nav-bar types
   */
  isGlobalNav: Ember.computed('navMode', function() {
    return this.get('navMode') === 'globalNav';
  }),

  /**
   * Fetch array of navbar items from configs
   */
  navItems: Ember.computed('navMode', function() {
    const env = Ember.getOwner(this).resolveRegistration('config:environment');
    return env.navigation[this.get('navMode')];
  })
});