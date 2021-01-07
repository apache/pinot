import Component from '@ember/component';
import { A as EmberArray } from '@ember/array';

import { parseRoot, parseSubtree } from 'thirdeye-frontend/utils/anomalies-tree-parser';
import pubSub from 'thirdeye-frontend/utils/pub-sub';

export default Component.extend({
  data: EmberArray(),
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

  /**
   * The callback function to invoke when anomaly is drilled down
   *
   * @public
   * @type {Function}
   */
  onDrilldown: () => {},

  /** Internal states */

  /**
   * Tracks the list of breadcrumbs
   *
   * @private
   * @type {Array<Object>}
   */
  breadcrumbList: EmberArray(),

  onAnomalyDrilldown(anomalyId) {
    this.set('data', parseSubtree(anomalyId, this.anomalies));
  },

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
   * Call the tree parser to fetch the component information for the root level and
   * subscribe to drilldown updates.
   *
   * @override
   */
  didReceiveAttrs() {
    this._super(...arguments);

    //Initial Processing
    const { output, breadcrumbInfo } = parseRoot(this.alertId, this.anomalies);

    this.breadcrumbList.push(breadcrumbInfo);
    this.set('data', output);

    //Processing on each drilldown
    const subscription = pubSub.subscribe('onAnomalyDrilldown', (anomalyId) => {
      const { output, breadcrumbInfo } = parseSubtree(anomalyId, this.anomalies);

      this.set('breadcrumbList', [...this.breadcrumbList, breadcrumbInfo]);
      this.set('data', output);

      this.onDrilldown();
    });

    this.set('subscription', subscription);
  },

  /**
   * Ember component life hook.
   * Delete the callback subscribed to for anomaly drilldowns
   *
   * @override
   */
  willDestroyElement() {
    this.subscription.unSubscribe();
    this._super(...arguments);
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
    onBreadcrumbClick({ id, isRoot = false }, index) {
      const { output } = isRoot ? parseRoot(id, this.anomalies) : parseSubtree(id, this.anomalies);

      this.set('data', output);
      this.set('breadcrumbList', this.breadcrumbList.splice(0, index + 1));

      this.onBreadcrumbClick(isRoot);
    }
  }
});
