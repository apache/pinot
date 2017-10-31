import Ember from 'ember';
import c3 from 'c3';
import d3 from 'd3';
import _ from 'lodash';
import moment from 'moment';

export default Ember.Component.extend({
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
      type: 'line' // 'point', 'region'
    }
  },

  tooltip: {
    format: {
      title: (d) => moment(d).format('MM/DD hh:mm a'),
      value: (val, ratio, id) => d3.format('.3s')(val)
    }
  },

  _makeDiffConfig() {
    const cache = this.get('_seriesCache') || {};
    const series = this.get('series') || {};
    console.log('cache', cache);
    console.log('series', series);

    const addedKeys = Object.keys(series).filter(sid => !(sid in cache));
    const changedKeys = Object.keys(series).filter(sid => sid in cache && !_.isEqual(cache[sid], series[sid]));
    const deletedKeys = Object.keys(cache).filter(sid => !(sid in series));
    const regionKeys = Object.keys(series).filter(sid => series[sid].type == 'region');
    console.log('addedKeys', addedKeys);
    console.log('changedKeys', changedKeys);
    console.log('deletedKeys', deletedKeys);
    console.log('regionKeys', deletedKeys);

    const regions = regionKeys.map(sid => {
      const t = series[sid].timestamps;
      return { axis: 'x', start: t[0], end: t[t.length - 1] };
    });
    console.log('regions', regions);

    const unloadKeys = changedKeys.concat(deletedKeys);
    const unload = unloadKeys.concat(unloadKeys.map(sid => sid + '-timestamps'));
    console.log('unload', unload);

    const loadKeys = addedKeys.concat(changedKeys).filter(sid => !regionKeys.includes(sid));
    const xs = {};
    loadKeys.forEach(sid => xs[sid] = sid + '-timestamps');
    console.log('xs', xs);

    const values = loadKeys.map(sid => [sid].concat(series[sid].values));
    console.log('values', values);

    const timestamps = loadKeys.map(sid => [sid + '-timestamps'].concat(series[sid].timestamps));
    console.log('timestamps', timestamps);

    const types = {};
    loadKeys.forEach(sid => types[sid] = series[sid].type);
    console.log('types', types);

    const columns = values.concat(timestamps);
    console.log('columns', columns);

    const tooltip = this.get('tooltip');
    console.log('tooltip', tooltip);

    const config = { unload, xs, columns, types, regions, tooltip };

    return config;
  },

  _updateCache() {
    const series = this.get('series');
    this.set('_seriesCache', _.cloneDeep(series));
  },

  didUpdateAttrs() {
    this._super(...arguments);
    console.log('didUpdateAttrs()');

    const diffConfig = this._makeDiffConfig();
    console.log('diffConfig', diffConfig);

    const chart = this.get('_chart');
    chart.regions(diffConfig.regions);
    chart.load(diffConfig);

    this._updateCache();
  },

  didInsertElement() {
    this._super(...arguments);
    console.log('didInsertElement()');

    const diffConfig = this._makeDiffConfig();

    const config = {};
    config.bindto = this.get('element');
    config.data = {
      xs: diffConfig.xs,
      columns: diffConfig.columns,
      types: diffConfig.types
    };
    config.regions = diffConfig.regions;
    config.tooltip = diffConfig.tooltip;
    console.log('config', config);

    this.set('_chart', c3.generate(config));

    this._updateCache();
  }
});
