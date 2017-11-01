import Ember from 'ember';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  timeseries: null, // {}

  series: Ember.computed(
    'entities',
    'timeseries',
    'selectedUrns',
    function () {
      const entities = this.get('entities');
      const timeseries = this.get('timeseries');
      const selectedUrns = this.get('selectedUrns');

      console.log('series: entities', entities);
      console.log('series: timeseries', timeseries);
      console.log('series: selectedUrns', selectedUrns);

      const series = {};
      [...selectedUrns]
        .filter(urn => urn in entities)
        .filter(urn => entities[urn].type != 'metric' || urn in timeseries)
        .forEach(urn => {
          const e = entities[urn];
          series[this.entityToLabel(e)] = this.entityToSeries(e);
        });
      console.log('series: series', series);

      return series;
    }
  ),

  entityToLabel (entity) {
    return entity.label;
  },

  entityToSeries (entity) {
    if (entity.type == 'metric') {
      const timeseries = this.get('timeseries');
      return {
        timestamps: timeseries[entity.urn].timestamps,
        values: timeseries[entity.urn].values,
        type: 'line',
        axis: 'y'
      };

    } else if (entity.type == 'dimension') {
      // TODO requires support for urn with metric id + filter
      return {};

    } else if (entity.type == 'event') {
      return {
        timestamps: [entity.start, entity.end],
        values: [1, 1],
        type: 'line',
        axis: 'y2'
      };
    }

  }
});
