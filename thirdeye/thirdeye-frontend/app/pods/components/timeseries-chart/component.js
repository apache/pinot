import Ember from 'ember';
import c3 from 'c3';
import _ from 'lodash';

export default Ember.Component.extend({
  tagName: 'div',
  classNames: ['timeseries-chart'],

  // internal
  _chart: null,
  _seriesCache: null,

  // wrapper
  series: {
    example_series: {
      timestamps: [0, 1, 2, 5, 6],
      values: [10, 10, 5, 27, 28],
      type: 'line' // 'point', 'region'
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
    console.log('addedKeys', addedKeys);
    console.log('changedKeys', changedKeys);
    console.log('deletedKeys', deletedKeys);

    const unloadKeys = changedKeys.concat(deletedKeys);
    const unload = unloadKeys.concat(unloadKeys.map(sid => sid + '-timestamps'));
    console.log('unload', unload);

    const xsKeys = addedKeys.concat(changedKeys);
    const xs = {};
    xsKeys.forEach(sid => xs[sid] = sid + '-timestamps');
    console.log('xs', xs);

    const columnsKeys = addedKeys.concat(changedKeys);
    const columns = columnsKeys.map(sid => [sid].concat(series[sid].values)).concat(
      columnsKeys.map(sid => [sid + '-timestamps'].concat(series[sid].timestamps))
    );
    console.log('columns', columns);

    const typesKeys = addedKeys.concat(changedKeys);
    const types = {};
    typesKeys.forEach(sid => types[sid] = series[sid].type);
    console.log('types', types);

    const config = { unload, xs, columns, types };

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
    console.log('config', config);

    this.set('_chart', c3.generate(config));

    this._updateCache();
  }
});
