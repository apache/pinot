import { computed } from '@ember/object';
import Component from '@ember/component';
import moment from 'moment';
import d3 from 'd3';
import buildTooltip from 'thirdeye-frontend/utils/build-tooltip';
import {
  toBaselineUrn,
  toMetricUrn,
  filterPrefix,
  hasPrefix,
  toMetricLabel,
  colorMapping
} from 'thirdeye-frontend/utils/rca-utils';

const TIMESERIES_MODE_ABSOLUTE = 'absolute';
const TIMESERIES_MODE_RELATIVE = 'relative';
const TIMESERIES_MODE_LOG = 'log';
const TIMESERIES_MODE_SPLIT = 'split';

const EVENT_MIN_GAP = 60000;

export default Component.extend({
  entities: null, // {}

  selectedUrns: null, // Set

  timeseries: null, // {}

  context: null, // {}

  onHover: null, // function (urns)

  timeseriesMode: null, // 'absolute', 'relative', 'log'

  classNames: ['rootcause-chart'],

  /**
   * id of the entity to be focused on the chart
   * @type {String}
   */
  focusedUrn: null,

  /**
   * Translate an urn into a chart id
   * @returns {String|null}
   */
  focusedId: computed('focusedUrn', function() {
    const urn = this.get('focusedUrn');

    return this._urnToChartId(urn);
  }),

  init() {
    this._super(...arguments);
    this.set('timeseriesMode', TIMESERIES_MODE_ABSOLUTE);
  },

  legend: {
    show: false
  },

  subchart: {
    show: true
  },

  colorMapping: colorMapping,

  /**
   * Adding the buildTooltip Template helper to the this context
   */
  buildTooltip: buildTooltip,

  /**
   * custom height for split mode
   */
  splitChartHeight: {
    height: 200
  },

  tooltip: computed(
    'onHover',
    function () {
      return {
        grouped: true,
        contents: (items, defaultTitleFormat, defaultValueFormat, color) => {
          const t = moment(items[0].x);
          const hoverUrns = this._onHover(t.valueOf());

          const {
            entities,
            timeseries
          } = this.getProperties('entities', 'timeseries');

          const tooltip = this.buildTooltip.create();

          return tooltip.compute({
            entities,
            timeseries,
            hoverUrns,
            hoverTimestamp: t.valueOf()
          });
        }
      };
    }
  ),

  axis: computed(
    'context',
    function () {
      const { context } = this.getProperties('context');

      const { analysisRange } = context;

      return {
        y: {
          show: true,
          tick: {
            format: d3.format('.2s')
          }
        },
        y2: {
          show: false,
          min: 0,
          max: 1
        },
        x: {
          type: 'timeseries',
          show: true,
          min: analysisRange[0],
          max: analysisRange[1],
          tick: {
            fit: false
          }
        }
      };
    }
  ),

  /**
   * Array of displayable urns in timeseries chart (events, metrics, metric refs)
   */
  displayableUrns: computed(
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

  /**
   * Transformed data series as ingested by timeseries-chart
   */
  series: computed(
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

      return this._makeChartSeries(displayableUrns);
    }
  ),

  //
  // split view logic
  //

  splitSubchart: {
    show: false
  },

  /**
   * Dictionary of transformed data series for split view, keyed by metric ref urn.
   */
  splitSeries: computed(
    'entities',
    'timeseries',
    'context',
    'displayableUrns',
    'timeseriesMode',
    function () {
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

  /**
   * Array of urns for split view. One per metric ref urn.
   */
  splitUrns: computed(
    'entities',
    'displayableUrns',
    'timeseriesMode',
    function () {
      const { entities, displayableUrns, timeseriesMode } =
        this.getProperties('entities', 'displayableUrns', 'timeseriesMode');

      if (timeseriesMode != TIMESERIES_MODE_SPLIT) {
        return {};
      }

      return filterPrefix(displayableUrns, 'frontend:metric:current:')
        .map(urn => [toMetricLabel(toMetricUrn(urn), entities), urn])
        .sort()
        .map(t => t[1]);
    }
  ),

  /**
   * Dictionary of chart labels for split view, keyed by metric ref urn.
   */
  splitLabels: computed(
    'entities',
    'displayableUrns',
    'timeseriesMode',
    function () {
      const { entities, displayableUrns, timeseriesMode } =
        this.getProperties('entities', 'displayableUrns', 'timeseriesMode');

      if (timeseriesMode != TIMESERIES_MODE_SPLIT) {
        return {};
      }

      return filterPrefix(displayableUrns, 'frontend:metric:current:')
        .reduce((agg, urn) => {
          agg[urn] = toMetricLabel(toMetricUrn(urn), entities);
          return agg;
        }, {});
    }
  ),

  /**
   * Split view indicator
   */
  isSplit: computed(
    'timeseriesMode',
    function () {
      return this.get('timeseriesMode') == TIMESERIES_MODE_SPLIT;
    }
  ),

  //
  // helpers
  //

  /**
   * Returns a dictionary with transformed data series as ingested by timeseries-chart for a given set of
   * urns (metric refs and event entities).
   *
   * @param {Array} urns metric ref urns
   */
  _makeChartSeries(urns) {
    const { context } = this.getProperties('context');
    const { anomalyRange } = context;

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

    return series;
  },

  /**
   * Dictionary for the min and mix timestamps of events and timeseries for hover bounds checking
   */
  _hoverBounds: computed(
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

  /**
   * Dictionary of y-values for event entities. Laid out by a swimlane algorithm purely for display purposes
   */
  _eventValues: computed(
    'entities',
    'displayableUrns',
    function () {
      const { entities, displayableUrns, context } =
        this.getProperties('entities', 'displayableUrns', 'context');

      const selectedEvents = filterPrefix(displayableUrns, 'thirdeye:event:').map(urn => entities[urn]);

      const analysisRangeEnd = context.analysisRange[1];
      const handleOngoingEnd = (e) => e.end <= 0 ? analysisRangeEnd : e.end;

      const starts = selectedEvents.map(e => [e.start, e.urn]);
      const ends = selectedEvents.map(e => [handleOngoingEnd(e) + EVENT_MIN_GAP, e.urn]); // no overlap
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
      Object.keys(urn2lane).forEach(urn => normalized[urn] = 1 - 0.5 * urn2lane[urn] / max);

      return normalized;
    }
  ),

  /**
   * Converts a urn into a valid series label as ingested by timeseries-chart
   *
   * @param {string} urn entity urn
   * @returns {string} label
   * @private
   */
  _makeLabel(urn) {
    return urn;
  },

  /**
   * Returns a transformed data series as ingested by timeseries-chart for a given urn. Supports
   * metric refs and event entities.
   *
   * @param {string} urn entity urn
   * @returns {Object} timeseries-chart series instance
   * @private
   */
  _makeSeries(urn) {
    const { entities, timeseries, timeseriesMode, _eventValues, context } =
      this.getProperties('entities', 'timeseries', 'timeseriesMode', '_eventValues', 'context');

    if (hasPrefix(urn, 'frontend:metric:current:')) {
      const metricEntity = entities[toMetricUrn(urn)];
      const series = {
        timestamps: timeseries[urn].timestamps,
        values: timeseries[urn].values,
        color: metricEntity ? metricEntity.color : 'none',
        type: 'line',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (hasPrefix(urn, 'frontend:metric:baseline:')) {
      const metricEntity = entities[toMetricUrn(urn)];
      const series = {
        timestamps: timeseries[urn].timestamps,
        values: timeseries[urn].values,
        color: metricEntity ? 'light-' + metricEntity.color : 'none',
        type: 'line',
        axis: 'y'
      };

      return this._transformSeries(timeseriesMode, series);

    } else if (hasPrefix(urn, 'thirdeye:event:')) {
      const val = _eventValues[urn];
      const endRange = context.analysisRange[1];
      const end = entities[urn].end <= 0 ? endRange : entities[urn].end;

      return {
        timestamps: [entities[urn].start, end],
        values: [val, val],
        color: entities[urn].color,
        type: 'line',
        axis: 'y2'
      };
    }
  },

  /**
   * Returns a timeseries transformed by mode.
   *
   * @param {string} mode transformation mode
   * @param {Object} series timeseries container ({ timestamps: [], values: [] })
   * @returns {Object} transformed timeseries container
   * @private
   */
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

  /**
   * Transforms the input timeseries to relative values based on first value.
   *
   * @param {Object} series timeseries container
   * @private
   */
  _transformSeriesRelative(series) {
    const first = series.values.filter(v => v)[0] || 1.0;
    const output = Object.assign({}, series);
    output.values = series.values.map(v => v != null ? 1.0 * v / first : null);
    return output;
  },

  /**
   * Transforms the input timeseries to log values, with a custom interpretation
   *
   * @param {Object} series timeseries container
   * @private
   */
  _transformSeriesLog(series) {
    const output = Object.assign({}, series);
    output.values = series.values.map(v => v != null ? this._customLog(v) : null);
    return output;
  },

  /**
   * Log transform helper for values <= 0
   *
   * @param {Float} v value
   * @private
   */
  _customLog(v) {
    if (v <= 0) { return null; }
    return Math.log(v);
  },

  /**
   * Returns the set of urns whose timeseries bounds include the timestamp d.
   *
   * @param {Int} d timestamp in millis
   * @private
   */
  _onHover(d) {
    const { _hoverBounds: bounds, displayableUrns, onHover } =
      this.getProperties('_hoverBounds', 'displayableUrns', 'onHover');

    if (onHover != null) {
      const urns = [...displayableUrns].filter(urn => bounds[urn] && bounds[urn][0] <= d && d <= bounds[urn][1]);

      const metricUrns = filterPrefix(urns, 'frontend:metric:');
      const eventUrns = filterPrefix(urns, 'thirdeye:event:');
      const outputUrns = new Set([...metricUrns, ...metricUrns.map(toMetricUrn), ...eventUrns]);

      onHover(outputUrns, d);
      return outputUrns;
    }
  },

  /**
   * Converts an urn into a id that's readable
   * by the c3-library .focus method
   * @param {String} urn focused entity's urn
   * @private
   */
  _urnToChartId(urn) {
    if (!urn) return;

    if (urn.includes('metric')) {
      var [, metric, id, ...filters] = urn.split(':');
      urn = ['frontend', metric, 'current', id].join(':');
      if (filters.length) {
        urn += `:${filters.join(':')}`;
      }
    }
    return urn;
  }
});
