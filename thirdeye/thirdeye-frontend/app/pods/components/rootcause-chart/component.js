import Ember from 'ember';
import d3 from 'd3';
import moment from 'moment';
import { toBaselineUrn, toMetricUrn, filterPrefix, hasPrefix, stripTail, toBaselineRange } from 'thirdeye-frontend/helpers/utils';

const TIMESERIES_MODE_ABSOLUTE = 'absolute';
const TIMESERIES_MODE_RELATIVE = 'relative';
const TIMESERIES_MODE_LOG = 'log';
const TIMESERIES_MODE_SPLIT = 'split';

export default Ember.Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  timeseries: null, // {}

  context: null, // {}

  onHover: null, // function (urns)

  timeseriesMode: null, // 'absolute', 'relative', 'log'

  init() {
    this._super(...arguments);
    this.set('timeseriesMode', TIMESERIES_MODE_ABSOLUTE);
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

  axis: Ember.computed(
    'context',
    function () {
      console.log('rootcause-chart: axis');
      const { context } = this.getProperties('context');
      
      const { analysisRange } = context; 
      
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

  displayableUrns: Ember.computed(
    'entities',
    'timeseries',
    'selectedUrns',
    function () {
      const { entities, timeseries, selectedUrns } =
        this.getProperties('entities', 'timeseries', 'selectedUrns');

      return filterPrefix(selectedUrns, ['thirdeye:event:', 'frontend:metric:'])
        .filter(urn => !hasPrefix(urn, 'thirdeye:event:') || entities[urn])
        .filter(urn => !hasPrefix(urn, 'frontend:metric:') || timeseries[urn]);
    }
  ),

  series: Ember.computed(
    'entities',
    'timeseries',
    'context',
    'displayableUrns',
    'timeseriesMode',
    function () {
      const { timeseries, timeseriesMode, displayableUrns } =
        this.getProperties('timeseries', 'timeseriesMode', 'displayableUrns');

      if (timeseriesMode == TIMESERIES_MODE_SPLIT) {
        return {};
      }

      console.log('rootcauseChart: series: timeseries', timeseries);
      console.log('rootcauseChart: series: displayableUrns', displayableUrns);
      return this._makeChartSeries(displayableUrns);
    }
  ),

  //
  // split view logic
  //

  splitSeries: Ember.computed(
    'entities',
    'timeseries',
    'context',
    'displayableUrns',
    'timeseriesMode',
    function () {
      console.log('rootcauseChart: splitSeries()');
      const { displayableUrns, timeseriesMode } =
        this.getProperties('displayableUrns', 'timeseriesMode');

      if (timeseriesMode != TIMESERIES_MODE_SPLIT) {
        return {};
      }

      const splitSeries = {};
      const metricUrns = new Set(filterPrefix(displayableUrns, 'frontend:metric:'));
      const otherUrns = new Set([...displayableUrns].filter(urn => !metricUrns.has(urn)));

      filterPrefix(metricUrns, ['frontend:metric:current:']).forEach(urn => {
        const splitMetricUrns = [urn];
        const baselineUrn = toBaselineUrn(urn);
        if (metricUrns.has(baselineUrn)) {
          splitMetricUrns.push(baselineUrn);
        }

        const splitUrns = new Set(splitMetricUrns.concat([...otherUrns]));

        splitSeries[urn] = this._makeChartSeries(splitUrns);
      });

      return splitSeries;
    }
  ),

  splitUrns: Ember.computed(
    'entities',
    'displayableUrns',
    'timeseriesMode',
    function () {
      console.log('rootcauseChart: splitUrns()');
      const { entities, displayableUrns, timeseriesMode } =
        this.getProperties('entities', 'displayableUrns', 'timeseriesMode');

      if (timeseriesMode != TIMESERIES_MODE_SPLIT) {
        return {};
      }

      return filterPrefix(displayableUrns, 'frontend:metric:current:')
        .map(urn => [entities[stripTail(toMetricUrn(urn))].label.split("::")[1], urn])
        .sort()
        .map(t => t[1]);
    }
  ),

  splitLabels: Ember.computed(
    'entities',
    'displayableUrns',
    'timeseriesMode',
    function () {
      console.log('rootcauseChart: splitLabels()');
      const { entities, displayableUrns, timeseriesMode } =
        this.getProperties('entities', 'displayableUrns', 'timeseriesMode');

      if (timeseriesMode != TIMESERIES_MODE_SPLIT) {
        return {};
      }

      return filterPrefix(displayableUrns, 'frontend:metric:current:')
        .reduce((agg, urn) => {
          agg[urn] = entities[stripTail(toMetricUrn(urn))].label.split("::")[1];
          return agg;
        }, {});
    }
  ),

  isSplit: Ember.computed(
    'timeseriesMode',
    function () {
      console.log('rootcauseChart: isSplit()');
      return this.get('timeseriesMode') == TIMESERIES_MODE_SPLIT;
    }
  ),

  //
  // helpers
  //

  _makeChartSeries(urns) {
    const { context } = this.getProperties('context');
    
    const { anomalyRange, compareMode } = context;
    const baselineRange = toBaselineRange(anomalyRange, compareMode);

    const series = {};
    [...urns].forEach(urn => {
      series[this._makeLabel(urn)] = this._makeSeries(urn);
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
  },

  _hoverBounds: Ember.computed(
    'entities',
    'timeseries',
    'displayableUrns',
    function () {
      const { displayableUrns } = this.getProperties('displayableUrns');

      const bounds = {};
      [...displayableUrns].forEach(urn => {
        const timestamps = this._makeSeries(urn).timestamps;
        bounds[urn] = [timestamps[0], timestamps[timestamps.length-1]];
      });

      return bounds;
    }
  ),

  _eventValues: Ember.computed(
    'entities',
    'displayableUrns',
    function () {
      const { entities, displayableUrns } =
        this.getProperties('entities', 'displayableUrns');

      const selectedEvents = filterPrefix(displayableUrns, 'thirdeye:event:').map(urn => entities[urn]);

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

  _makeLabel(urn) {
    return urn;
  },

  _makeSeries(urn) {
    const { entities, timeseries, timeseriesMode, _eventValues } =
      this.getProperties('entities', 'timeseries', 'timeseriesMode', '_eventValues');

    if (hasPrefix(urn, 'frontend:metric:current:')) {
      const series = {
        timestamps: timeseries[urn].timestamps,
        values: timeseries[urn].values,
        type: 'line',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (hasPrefix(urn, 'frontend:metric:baseline:')) {
      const series = {
        timestamps: timeseries[urn].timestamps,
        values: timeseries[urn].values,
        type: 'scatter',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (hasPrefix(urn, 'thirdeye:event:')) {
      const val = _eventValues[urn];
      return {
        timestamps: [entities[urn].start, entities[urn].end || entities[urn].start],
        values: [val, val],
        type: 'line',
        axis: 'y2'
      };
    }
  },

  _transformSeries(mode, series) {
    switch(mode) {
      case TIMESERIES_MODE_ABSOLUTE:
        return series; // raw data
      case TIMESERIES_MODE_RELATIVE:
        return this._transformSeriesRelative(series);
      case TIMESERIES_MODE_LOG:
        return this._transformSeriesLog(series);
      case TIMESERIES_MODE_SPLIT:
        return series; // raw data
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
    const { _hoverBounds: bounds, displayableUrns, onHover } =
      this.getProperties('_hoverBounds', 'displayableUrns', 'onHover');

    if (onHover != null) {
      const urns = [...displayableUrns].filter(urn => bounds[urn] && bounds[urn][0] <= d && d <= bounds[urn][1]);
      onHover(urns);
    }
  }
});
