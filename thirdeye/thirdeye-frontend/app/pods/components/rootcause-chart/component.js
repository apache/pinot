import Ember from 'ember';
import d3 from 'd3';
import moment from 'moment';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  timeseries: null, // {}

  onHover: null, // function (urns)

  anomalyRange: null, // [2]

  baselineRange: null, // [2]

  analysisRange: null, // [2]

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

  legend: {
    show: false
  },

  series: Ember.computed(
    'entities',
    'timeseries',
    'selectedUrns',
    'anomalyRange',
    'baselineRange',
    function () {
      const entities = this.get('entities');
      const timeseries = this.get('timeseries');
      const selectedUrns = this.get('selectedUrns');

      const series = {};
      [...selectedUrns]
        .filter(urn => urn in entities)
        .filter(urn => ['metric', 'event'].includes(entities[urn].type))
        .filter(urn => entities[urn].type != 'metric' || urn in timeseries)
        .forEach(urn => {
          const e = entities[urn];
          series[this._entityToLabel(e)] = this._entityToSeries(e);
        });

      const anomalyRange = this.get('anomalyRange');
      series['anomalyRange'] = {
        timestamps: anomalyRange,
        values: [0, 0],
        type: 'region',
        color: 'orange'
      };

      const baselineRange = this.get('baselineRange');
      series['baselineRange'] = {
        timestamps: baselineRange,
        values: [0, 0],
        type: 'region',
        color: 'blue'
      };

      console.log('rootcause-chart: series: series', series);

      return series;
    }
  ),

  axis: Ember.computed(
    'analysisRange',
    function () {
      const analysisRange = this.get('analysisRange');
      return {
        y: {
          show: true
        },
        y2: {
          show: false
        },
        x: {
          type: 'timeseries',
          show: true,
          min: analysisRange[0],
          max: analysisRange[1],
          tick: {
            format: '%Y-%m-%d'
          }
        }
      };
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
        .filter(urn => ['metric', 'event'].includes(entities[urn].type))
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
    return entity.urn;
  },

  _entityToSeries(entity) {
    if (entity.type == 'metric') {
      const timeseries = this.get('timeseries');
      // console.log(entity.urn, 'timestamps', timeseries[entity.urn].timestamps.length, 'values', timeseries[entity.urn].values.length);
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
      // console.log(entity.urn, 'timestamps', entity.start, entity.end);
      return {
        timestamps: [entity.start, entity.end || entity.start],
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
