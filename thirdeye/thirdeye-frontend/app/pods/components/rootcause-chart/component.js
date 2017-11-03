import Ember from 'ember';
import d3 from 'd3';
import moment from 'moment';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  timeseries: null, // {}

  onHover: null, // function (urns)

  tooltip: Ember.computed(
    'onHover',
    function () {
      const onHover = this.get('onHover');
      return {
        format: {
          title: (d) => {
            this._onHover(d);
            return moment(d).format('MM/DD hh:mm a');
          },
          value: (val, ratio, id) => d3.format('.3s')(val)
        }
      };
    }
  ),

  series: Ember.computed(
    'entities',
    'timeseries',
    'selectedUrns',
    function () {
      const entities = this.get('entities');
      const timeseries = this.get('timeseries');
      const selectedUrns = this.get('selectedUrns');

      const series = {};
      [...selectedUrns]
        .filter(urn => urn in entities)
        .filter(urn => entities[urn].type != 'metric' || urn in timeseries)
        .forEach(urn => {
          const e = entities[urn];
          series[this._entityToLabel(e)] = this._entityToSeries(e);
        });

      console.log('rootcause-chart: series: series', series);

      return series;
    }
  ),

  _hoverBounds: Ember.computed(
    'entities',
    'timeseries',
    'selectedUrns',
    function () {
      const entities = this.get('entities');
      const timeseries = this.get('timeseries');
      const selectedUrns = this.get('selectedUrns');

      const bounds = {};
      [...selectedUrns]
        .filter(urn => urn in entities)
        .filter(urn => entities[urn].type != 'metric' || urn in timeseries)
        .forEach(urn => {
          const e = entities[urn];
          const timestamps = this._entityToSeries(e).timestamps;
          bounds[urn] = [timestamps[0], timestamps[timestamps.length-1]];
        });

      return bounds;
    }
  ),

  _entityToLabel(entity) {
    return entity.label;
  },

  _entityToSeries(entity) {
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
  },

  _onHover(d) {
    const bounds = this.get('_hoverBounds');
    const selectedUrns = this.get('selectedUrns');
    const onHover = this.get('onHover');
    if (onHover != null) {
      console.log('rootcause-chart: _onHover: bounds', bounds);
      const urns = [...selectedUrns].filter(urn => bounds[urn] && bounds[urn][0] <= d && d <= bounds[urn][1]);
      onHover(urns);
    }
  },

  actions: {
    // NOTE: not passed through yet
    onMouseOut() {
      const onHover = this.get('onHover');
      if (onHover != null) {
        console.log('rootcause-chart: onMouseOut()');
        onHover([]);
      }
    }
  }

});
