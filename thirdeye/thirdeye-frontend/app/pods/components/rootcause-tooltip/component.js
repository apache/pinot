import Component from '@ember/component';
import { computed } from '@ember/object';
import { filterPrefix, toBaselineUrn, toCurrentUrn, toMetricLabel, stripTail } from 'thirdeye-frontend/helpers/utils';

export default Component.extend({
  entities: null, // {}

  timeseries: null, // {}

  hoverUrns: null, // Set

  hoverTimestamp: null, // 0

  urns: computed(
    'hoverUrns',
    function () {
      const { hoverUrns } = this.getProperties('hoverUrns');
      const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
      const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');
      return [...metricUrns, ...eventUrns];
    }
  ),

  labels: computed(
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

  values: computed(
    'timeseriesLookup',
    'hoverUrns',
    'hoverTimestamp',
    function () {
      const { timeseriesLookup, hoverUrns, hoverTimestamp } =
        this.getProperties('timeseriesLookup', 'hoverUrns', 'hoverTimestamp');
      const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
      const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');

      const values = {};
      metricUrns.forEach(urn => {
        const currentLookup = timeseriesLookup[toCurrentUrn(urn)] || [];
        const currentTimeseries = currentLookup.find(t => t[0] >= hoverTimestamp);
        const current = currentTimeseries ? currentTimeseries[1] : parseFloat('NaN');

        const baselineLookup = timeseriesLookup[toBaselineUrn(urn)] || [];
        const baselineTimeseries = baselineLookup.find(t => t[0] >= hoverTimestamp);
        const baseline = baselineTimeseries ? baselineTimeseries[1] : parseFloat('NaN');

        const change = current / baseline - 1;

        values[urn] = `${this.format(current)} / ${this.format(baseline)} (${change > 0 ? '+' : ''}${this.format(change * 100)}%)`;
      });

      eventUrns.forEach(urn => values[urn] = '');

      return values;
    }
  ),

  colors: computed(
    'entities',
    'hoverUrns',
    function () {
      const { entities, hoverUrns } = this.getProperties('entities', 'hoverUrns');

      return filterPrefix(hoverUrns, ['thirdeye:metric:', 'thirdeye:event:'])
        .filter(urn => entities[stripTail(urn)])
        .reduce((agg, urn) => {
          agg[urn] = entities[stripTail(urn)].color;
          return agg;
        }, {});
    }
  ),

  timeseriesLookup: computed(
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
  ),

  format(f) {
    const fixed = Math.max(3 - Math.max(Math.floor(Math.log10(f)) + 1, 0), 0);
    return f.toFixed(fixed);
  }

});
