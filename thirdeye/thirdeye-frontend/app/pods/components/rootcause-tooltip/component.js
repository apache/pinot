import Component from '@ember/component';
import { filterPrefix, toBaselineUrn, toCurrentUrn, toMetricLabel } from 'thirdeye-frontend/helpers/utils';

export default Component.extend({
  entities: null, // {}

  timeseries: null, // {}

  hoverUrns: null, // Set

  hoverTimestamp: null, // 0

  urns: Ember.computed(
    'hoverUrns',
    function () {
      const { hoverUrns } = this.getProperties('hoverUrns');
      const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
      const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');
      return [...metricUrns, ...eventUrns];
    }
  ),

  labels: Ember.computed(
    'entities',
    'hoverUrns',
    function () {
      const { entities, hoverUrns } = this.getProperties('entities', 'hoverUrns');
      const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
      const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');

      const labels = {};
      metricUrns.forEach(urn => labels[urn] = toMetricLabel(urn, entities));
      eventUrns.forEach(urn => labels[urn] = entities[urn].label);

      return labels;
    }
  ),

  values: Ember.computed(
    'timeseriesLookup',
    'hoverUrns',
    'hoverTimestamp',
    function () {
      const { timeseriesLookup, hoverUrns, hoverTimestamp } =
        this.getProperties('timeseriesLookup', 'hoverUrns', 'hoverTimestamp');
      const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
      const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');

      console.log('rootcauseTooltip: values: timeseriesLookup', timeseriesLookup);

      const values = {};
      metricUrns.forEach(urn => {
        const current = timeseriesLookup[toCurrentUrn(urn)].find(t => t[0] >= hoverTimestamp)[1];
        const baseline = timeseriesLookup[toBaselineUrn(urn)].find(t => t[0] >= hoverTimestamp)[1];
        values[urn] = `${current} / ${baseline} (${current / baseline - 1})`;
      });

      return values;
    }
  ),

  timeseriesLookup: Ember.computed(
    'timeseries',
    'hoverUrns',
    function () {
      const { timeseries, hoverUrns } = this.getProperties('timeseries', 'hoverUrns');
      const frontendUrns = filterPrefix(hoverUrns, 'frontend:metric:');

      const lookup = {};
      frontendUrns.forEach(urn => {
        const ts = timeseries[urn];
        lookup[urn] = ts.timestamps.map((t, i) => [t, ts.values[i]]);
      });

      return lookup;
    }
  )

});
