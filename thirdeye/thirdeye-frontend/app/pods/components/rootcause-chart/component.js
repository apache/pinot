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

  timeseriesMode: null, // 'absolute', 'relative', 'log'

  init() {
    this._super(...arguments);
    this.set('timeseriesMode', 'absolute');
  },

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
    'timeseriesMode',
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
          show: false,
          min: 0,
          max: 1
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

  _eventValues: Ember.computed(
    'entities',
    'selectedUrns',
    'analysisRange',
    function () {
      const { entities, selectedUrns } =
        this.getProperties('entities', 'selectedUrns');

      const selectedEvents = [...selectedUrns].filter(urn => entities[urn] && entities[urn].type == 'event').map(urn => entities[urn]);

      const starts = selectedEvents.map(e => [e.start, e.urn]);
      const ends = selectedEvents.map(e => [e.end + 1, e.urn]); // no overlap
      const sorted = starts.concat(ends).sort();

      //
      // automated layouting for event time ranges based on 'swimlanes'.
      // events are assigned to different lanes such that their time ranges do not overlap visually
      // the swimlanes are then converted to y values between [0.0, 1.0]
      //
      const lanes = {};
      const urn2lane = {};
      let max = 10; // default value
      sorted.forEach(t => {
        const urn = t[1];

        if (!(urn in urn2lane)) {
          // add
          let i;
          for (i = 0; (i in lanes); i++);
          lanes[i] = urn;
          urn2lane[urn] = i;
          max = i > max ? i : max;

        } else {
          // remove
          delete lanes[urn2lane[urn]];

        }
      });

      const normalized = {};
      Object.keys(urn2lane).forEach(urn => normalized[urn] = 1 - 1.0 * urn2lane[urn] / max);

      return normalized;
    }
  ),

  _filterDisplayable(urns) {
    const { entities, timeseries } = this.getProperties('entities', 'timeseries');

    return [...urns]
      .filter(urn => entities[urn] && ['metric', 'event', 'frontend:baseline:metric'].includes(entities[urn].type))
      .filter(urn => (entities[urn].type != 'metric' && entities[urn].type != 'frontend:baseline:metric') || timeseries[urn]);
  },

  _entityToLabel(entity) {
    return entity.urn;
  },

  _entityToSeries(entity) {
    if (entity.type == 'metric') {
      const { timeseries, timeseriesMode } = this.getProperties('timeseries', 'timeseriesMode');

      const series = {
        timestamps: timeseries[entity.urn].timestamps,
        values: timeseries[entity.urn].values,
        type: 'line',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (entity.type == 'frontend:baseline:metric') {
      const { timeseries, timeseriesMode } = this.getProperties('timeseries', 'timeseriesMode');

      const series = {
        timestamps: timeseries[entity.urn].timestamps,
        values: timeseries[entity.urn].values,
        type: 'scatter',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (entity.type == 'dimension') {
      // TODO requires support for urn with metric id + filter
      return {};

    } else if (entity.type == 'event') {
      const { _eventValues } = this.getProperties('_eventValues');
      // console.log(entity.urn, 'timestamps', entity.start, entity.end);
      const val = _eventValues[entity.urn];
      return {
        timestamps: [entity.start, entity.end || entity.start],
        values: [val, val],
        type: 'line',
        axis: 'y2'
      };
    }
  },

  _transformSeries(mode, series) {
    switch(mode) {
      case 'absolute':
        return series; // raw data
      case 'relative':
        return this._transformSeriesRelative(series);
      case 'log':
        return this._transformSeriesLog(series);
    }
    return series;
  },

  _transformSeriesRelative(series) {
    const first = series.values.filter(v => v)[0];
    const output = Object.assign({}, series);
    output.values = series.values.map(v => 1.0 * v / first);
    return output;
  },

  _transformSeriesLog(series) {
    const output = Object.assign({}, series);
    output.values = series.values.map(v => Math.log(v));
    return output;
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
