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

  legend: {
    show: false
  },

  tooltip: Ember.computed(
    'onHover',
    function () {
      const { onHover } = this.getProperties('onHover');

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
    'anomalyRange',
    'baselineRange',
    function () {
      const { entities, selectedUrns, anomalyRange, baselineRange } =
        this.getProperties('entities', 'selectedUrns', 'anomalyRange', 'baselineRange');

      const series = {};
      this._filterDisplayable(selectedUrns)
        .forEach(urn => {
          const e = entities[urn];
          series[this._entityToLabel(e)] = this._entityToSeries(e);
        });

      series['anomalyRange'] = {
        timestamps: anomalyRange,
        values: [0, 0],
        type: 'region',
        color: 'orange'
      };

      series['baselineRange'] = {
        timestamps: baselineRange,
        values: [0, 0],
        type: 'region',
        color: 'blue'
      };

      return series;
    }
  ),

  axis: Ember.computed(
    'analysisRange',
    function () {
      console.log('rootcause-chart: axis');
      const { analysisRange } = this.getProperties('analysisRange');
      console.log('rootcause-chart: axis: analysisRange', analysisRange);

      return {
        y: {
          show: true
        },
        y2: {
          show: false
        },
        x: {
          type: 'timeseries',
          show: true, // TODO false prevents function call, other option?
          min: analysisRange[0],
          max: analysisRange[1],
          tick: {
            count: Math.ceil(moment.duration(analysisRange[1] - analysisRange[0]).asDays()),
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
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');

      const bounds = {};
      this._filterDisplayable(selectedUrns)
        .forEach(urn => {
          const e = entities[urn];
          const timestamps = this._entityToSeries(e).timestamps;
          bounds[urn] = [timestamps[0], timestamps[timestamps.length-1]];
        });

      return bounds;
    }
  ),

  _filterDisplayable(urns) {
    const { entities, timeseries } = this.getProperties('entities', 'timeseries');

    return [...urns]
      .filter(urn => entities[urn])
      .filter(urn => ['metric', 'event', 'frontend:baseline:metric'].includes(entities[urn].type))
      .filter(urn => (entities[urn].type != 'metric' && entities[urn].type != 'frontend:baseline:metric') || timeseries[urn]);
  },

  _entityToLabel(entity) {
    return entity.urn;
  },

  _entityToSeries(entity) {
    if (entity.type == 'metric') {
      const { timeseries } = this.getProperties('timeseries');

      return {
        timestamps: timeseries[entity.urn].timestamps,
        values: timeseries[entity.urn].values,
        type: 'line',
        axis: 'y'
      };

    } else if (entity.type == 'frontend:baseline:metric') {
      const { timeseries } = this.getProperties('timeseries');

      return {
        timestamps: timeseries[entity.urn].timestamps,
        values: timeseries[entity.urn].values,
        type: 'scatter',
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
    const { _hoverBounds: bounds, selectedUrns, onHover } =
      this.getProperties('_hoverBounds', 'selectedUrns', 'onHover');

    if (onHover != null) {
      const urns = [...selectedUrns].filter(urn => bounds[urn] && bounds[urn][0] <= d && d <= bounds[urn][1]);
      onHover(urns);
    }
  }
});
