import Ember from 'ember';

export default Ember.Component.extend({
  breakdowns: null, // {}

  currentUrns: null, // Set

  current2baseline: null, // {}

  currentUrn: null, // ""

  current: Ember.computed(
    'breakdowns',
    'currentUrn',
    function () {
      const { breakdowns, currentUrn } = this.getProperties('breakdowns', 'currentUrn');

      const breakdown = breakdowns[currentUrn];

      if (!currentUrn || !breakdown) {
        return {};
      }
      return breakdown;
    }
  ),

  baseline: Ember.computed(
    'breakdowns',
    'current2baseline',
    'currentUrn',
    function () {
      const { breakdowns, current2baseline, currentUrn } =
        this.getProperties('breakdowns', 'current2baseline', 'currentUrn');

      const baselineUrn = current2baseline[currentUrn];
      const breakdown = breakdowns[baselineUrn];

      if (!currentUrn || !baselineUrn || !breakdown) {
        return {};
      }
      return breakdown;
    }
  )
});
