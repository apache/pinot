import Ember from 'ember';

export default Ember.Component.extend({
  aggregates: null, // {}

  currentUrns: null, // Set

  current2baseline: null, // {}

  currentUrn: null, // ""

  current: Ember.computed(
    'aggregates',
    'currentUrn',
    function () {
      const { aggregates, currentUrn } = this.getProperties('aggregates', 'currentUrn');

      const aggregate = aggregates[currentUrn];

      if (!currentUrn || !aggregate) {
        return {};
      }
      return aggregate;
    }
  ),

  baseline: Ember.computed(
    'aggregates',
    'current2baseline',
    'currentUrn',
    function () {
      const { aggregates, current2baseline, currentUrn } =
        this.getProperties('aggregates', 'current2baseline', 'currentUrn');

      const baselineUrn = current2baseline[currentUrn];
      const aggregate = aggregates[baselineUrn];

      if (!currentUrn || !baselineUrn || !aggregate) {
        return {};
      }
      return aggregate;
    }
  )
});
