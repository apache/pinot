import { debounce } from '@ember/runloop';
import Component from '@ember/component';
import c3 from 'c3';
import d3 from 'd3';
import _ from 'lodash';
import moment from 'moment';

export default Component.extend({
  tagName: 'div',
  classNames: ['timeseries-chart'],

  // internal
  _chart: null,
  _seriesCache: null,

  // external
  series: {
    example_series: {
      timestamps: [0, 1, 2, 5, 6],
      values: [10, 10, 5, 27, 28],
      type: 'line', // 'scatter', 'region'
      color: 'green'
    }
  },

  /**
   * Id of the entity to focus
   * @type {String}
   */
  focusedId: null,

  /**
   * default height property for the chart
   * May be overridden
   */
  height: {
    height: 400
  },

  tooltip: {
    format: {
      title: (d) => moment(d).format('MM/DD hh:mm a'),
      value: (val, ratio, id) => d3.format('.3s')(val)
    }
  },

  legend: {},

  axis: {
    y: {
      show: true
    },
    y2: {
      show: false
    },
    x: {
      type: 'timeseries',
      show: true,
      tick: {
        count: 5,
        format: '%Y-%m-%d'
      }
    }
  },

  /**
   * Converts color strings into their rgba equivalent
   */
  colorMapping: {
    blue: '#0091CA',
    green: '#469A1F',
    red: '#FF2C33',
    purple: '#827BE9',
    orange: '#E55800',
    teal: '#0E95A0',
    pink: '#FF1B90'
  },

  subchart: { // on init only
    show: true
  },

  zoom: { // on init only
    enabled: true
  },

  point: { // on init only
    show: true
  },

  line: { // on init only
    connectNull: true
  },

  _makeDiffConfig() {
    const cache = this.get('_seriesCache') || {};
    const series = this.get('series') || {};
    const colorMapping = this.get('colorMapping');
    const { axis, legend, tooltip, focusedId } = this.getProperties('axis', 'legend', 'tooltip', 'focusedId');

    const seriesKeys = Object.keys(series).sort();

    const addedKeys = seriesKeys.filter(sid => !cache[sid]);
    const changedKeys = seriesKeys.filter(sid => cache[sid] && !_.isEqual(cache[sid], series[sid]));
    const deletedKeys = Object.keys(cache).filter(sid => !series[sid]);
    const regionKeys = seriesKeys.filter(sid => series[sid] && series[sid].type == 'region');

    const regions = regionKeys.map(sid => {
      const t = series[sid].timestamps;
      let region = { start: t[0], end: t[t.length - 1] };

      if ('color' in series[sid]) {
        region.class = `c3-region--${series[sid].color}`;
      }

      return region;
    });

    const unloadKeys = deletedKeys.concat(regionKeys);
    const unload = unloadKeys.concat(unloadKeys.map(sid => `${sid}-timestamps`));

    const loadKeys = addedKeys.concat(changedKeys).filter(sid => !regionKeys.includes(sid));
    const xs = {};
    loadKeys.forEach(sid => xs[sid] = `${sid}-timestamps`);

    const values = loadKeys.map(sid => [sid].concat(series[sid].values));

    const timestamps = loadKeys.map(sid => [`${sid}-timestamps`].concat(series[sid].timestamps));

    const columns = values.concat(timestamps);

    const colors = {};
    loadKeys.filter(sid => series[sid].color).forEach(sid => colors[sid] = colorMapping[series[sid].color]);

    const types = {};
    loadKeys.filter(sid => series[sid].type).forEach(sid => types[sid] = series[sid].type);

    const axes = {};
    loadKeys.filter(sid => 'axis' in series[sid]).forEach(sid => axes[sid] = series[sid].axis);

    const config = { unload, xs, columns, types, regions, tooltip, focusedId, colors, axis, axes, legend };
    return config;
  },

  _makeAxisRange(axis) {
    const range = { min: {}, max: {} };
    Object.keys(axis).filter(key => 'min' in axis[key]).forEach(key => range['min'][key] = axis[key]['min']);
    Object.keys(axis).filter(key => 'max' in axis[key]).forEach(key => range['max'][key] = axis[key]['max']);
    return range;
  },

  _updateCache() {
    // debounce: do not trigger if chart object already destroyed
    if (this.isDestroyed) { return; }

    const series = this.get('series') || {};
    this.set('_seriesCache', _.cloneDeep(series));
  },

  /**
   * Updates the focused entity on the chart
   */
  _updateFocusedEntity: function() {
    const id = this.get('focusedId');

    if (id) {
      this._focus(id);
    } else {
      this._revert();
    }
  },

  /**
   * Focuses the entity associated with the provided id
   * @private
   * @param {String} id
   */
  _focus(id) {
    this.get('_chart').focus(id);
  },

  /**
   * Reverts the chart back to its original state
   * @private
   */
  _revert() {
    this.get('_chart').revert();
  },

  _updateChart() {
    const diffConfig = this._makeDiffConfig();
    const chart = this.get('_chart');
    chart.regions(diffConfig.regions);
    chart.axis.range(this._makeAxisRange(diffConfig.axis));
    chart.load(diffConfig);
    this._updateCache();
  },

  didUpdateAttrs() {
    this._super(...arguments);
    const series = this.get('series') || {};
    const cache = this.get('cache') || {};

    this._updateFocusedEntity();

    if (!_.isEqual(series, cache)) {
      debounce(this, this._updateChart, 300);
    }
  },

  didInsertElement() {
    this._super(...arguments);

    const diffConfig = this._makeDiffConfig();
    const config = {};
    config.bindto = this.get('element');
    config.data = {
      xs: diffConfig.xs,
      columns: diffConfig.columns,
      types: diffConfig.types,
      colors: diffConfig.colors,
      axes: diffConfig.axes
    };
    config.axis = diffConfig.axis;
    config.regions = diffConfig.regions;
    config.tooltip = diffConfig.tooltip;
    config.focusedId = diffConfig.focusedId;
    config.legend = diffConfig.legend;
    config.subchart = this.get('subchart');
    config.zoom = this.get('zoom');
    config.size = this.get('height');
    config.point = this.get('point');
    config.line = this.get('line');

    this.set('_chart', c3.generate(config));
    this._updateCache();
  }
});
