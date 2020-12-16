import Component from '@ember/component';
import { A as EmberArray } from '@ember/array';

export default Component.extend({
  /**
   * @public
   * @templateProps
   *
   * @type {Array}
   * @default [] */
  items: EmberArray(),

  /**
   * The callback function to invoke when breadcrumb is clicked
   *
   * @public
   * @type {Function}
   *
   * @param {Object} breadcrumb
   *   The information about the breadcrumb
   *
   * @param {Number} index
   *   The index of the breadcrumb that was clicked
   */
  onBreadcrumbClick: () => {},

  /**
   * Ember component life hook.
   *
   * @override
   */
  didReceiveAttrs() {
    this._super(...arguments);
    this.set('lastItemIndex', this.items.length - 1);
  },

  /**
   * Event handlers
   */
  actions: {
    /**
     * Call the parent callback when a breadcrumb is clicked
     *
     * @param {Object} breadcrumb
     *   The information about the breadcrumb object
     *
     * @param {Number} index
     *   The index of the breadcrumb that was clicked
     */
    onBreadcrumbClick(breadcrumb, index) {
      if (index !== this.lastItemIndex) {
        this.onBreadcrumbClick(breadcrumb, index);
      }
    }
  }
});
