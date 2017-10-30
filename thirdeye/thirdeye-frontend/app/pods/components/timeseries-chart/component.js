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
  series: null,

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

    const config = {};
    config['unload'] = unload;
    config['xs'] = xs;
    config['columns'] = columns;

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
      columns: diffConfig.columns
    };

    console.log('config', config);

    this.set('_chart', c3.generate(config));

    this._updateCache();
  }
});
