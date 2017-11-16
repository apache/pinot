import Ember from 'ember';
import { toCurrentUrn, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';

export default Ember.Component.extend({
  breakdowns: null, // {}

  selectedUrns: null, // Set

  currentUrn: null, // ""

  urns: Ember.computed(
    'selectedUrns',
    function () {
      const { selectedUrns } = this.getProperties('selectedUrns');
      return filterPrefix(selectedUrns, 'thirdeye:metric:');
    }
  ),

  current: Ember.computed(
    'breakdowns',
    'currentUrn',
    'currentUrns',
    function () {
      const { breakdowns, currentUrn, currentUrns } =
        this.getProperties('breakdowns', 'currentUrn', 'currentUrns');

      console.log('rootcauseHeatmap: current: breakdowns currentUrn', breakdowns, currentUrn);
      if (!currentUrn) {
        return {};
      }
      const breakdown = breakdowns[toCurrentUrn(currentUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  /**
   * Action to be passed into component
   * @type {Function}
   */
  onHeatmapClick: () => {},

  baseline: Ember.computed(
    'breakdowns',
    'currentUrn',
    function () {
      const { breakdowns, currentUrn } =
        this.getProperties('breakdowns', 'currentUrn');

      console.log('rootcauseHeatmap: baseline: breakdowns currentUrn', breakdowns, currentUrn);
      if (!currentUrn) {
        return {};
      }
      const breakdown = breakdowns[toBaselineUrn(currentUrn)];

      if (!breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  actions: {
    /**
     * Bubbles the action to the parent
     */
    onHeatmapClick() {
      this.attrs.onHeatmapClick(...arguments);
    }
  }
});
