import Component from '@ember/component';
import { A as EmberArray } from '@ember/array';

import { parseRoot } from 'thirdeye-frontend/utils/anomalies-tree-parser';

export default Component.extend({
  /**
   * @public
   * @templateProps
   *
   * @type {Array}
   * @default [] */
  anomalies: EmberArray(),

  /**
   * @public
   * @templateProps
   *
   * @type {Number}
   * @default undefined
   */
  alertId: undefined,

  /**
   * The callback function to invoke when breadcrumb is clicked
   *
   * @public
   * @type {Function}
   *
   * @param {Boolean} isRoot
   *   Whether we are exploring root level of the tree
   */
  onBreadcrumbClick: () => {},

  /** Internal states */

  /**
   * Tracks the list of breadcrumbs
   *
   * @private
   * @type {Array<Object>}
   */
  breadcrumbList: EmberArray(),

  /**
   * Ember component life hook.
   *
   * @override
   */
  init() {
    this._super(...arguments);
  },

  /**
   * Ember component life hook.
   * Call the tree parser to fetch the component information for the root level
   *
   * @override
   */
  didReceiveAttrs() {
    this._super(...arguments);

    const { breadcrumbInfo } = parseRoot(this.alertId, this.anomalies);

    this.breadcrumbList.push(breadcrumbInfo);
  },
  /**
   * Event handlers
   */
  actions: {
    /**
     * Call the tree parser to fetch the component subtree information
     * corresponding to the breadcrumb clicked
     *
     * @param {Object} breadcrumb
     *   The information about the breadcrumb object
     *
     * @param {Number} index
     *   The index of the breadcrumb that was clicked
     */
    onBreadcrumbClick({ isRoot = false }, index) {
      this.set('breadcrumbList', this.breadcrumbList.splice(0, index + 1));

      this.onBreadcrumbClick(isRoot);
    }
  }
});
