import Ember from 'ember';
import _ from 'lodash';

const extractTimeseries = (json) => {
  const timeseries = {};
  Object.keys(json).forEach(range =>
    Object.keys(json[range]).filter(sid => sid != 'timestamp').forEach(sid => {
      const urn = `thirdeye:metric:${sid}`;
      const jrng = json[range];
      const jval = jrng[sid];

      const timestamps = [];
      const values = [];
      jrng.timestamp.forEach((t, i) => {
        if (jval[i] != null) {
          timestamps.push(t);
          values.push(jval[i]);
        }
      });

      timeseries[urn] = {
        timestamps: timestamps,
        values: values
      };
    })
  );
  return timeseries;
};

const isIterable = (obj) => {
  if (obj == null || _.isString(obj)) {
    return false;
  }
  return typeof obj[Symbol.iterator] === 'function';
};

const makeIterable = (obj) => {
  if (obj == null) {
    return [];
  }
  return isIterable(obj) ? [...obj] : [obj];
};

export default Ember.Controller.extend({
  selectedUrns: null, // Set

  filteredUrns: null, // Set

  invisibleUrns: null, // Set

  _timeseriesCache: null, // {}

  _entitiesCache: null, // {}

  _pendingRequests: null, // {}

  entities: Ember.computed(
    '_entitiesLoader',
    '_entitiesCache',
    function () {
      console.log('entities()');
      this.get('_entitiesLoader'); // trigger loader. hacky
      return this.get('_entitiesCache') || {};
    }
  ),

  timeseries: Ember.computed(
    '_timeseriesLoader',
    '_timeseriesCache',
    function () {
      console.log('timeseries()');
      this.get('_timeseriesLoader'); // trigger loader. hacky
      return this.get('_timeseriesCache') || {};
    }
  ),

  chartSelectedUrns: Ember.computed(
    'model.primaryMetricUrn',
    'selectedUrns',
    'invisibleUrns',
    function () {
      console.log('chartSelectedUrns()');
      const selectedUrns = this.get('selectedUrns');
      const invisibleUrns = this.get('invisibleUrns');
      const primaryMetricUrn = this.get('model.primaryMetricUrn');

      // console.log('chartSelectedUrns: selectedUrns', selectedUrns);
      // console.log('chartSelectedUrns: invisibleUrns', invisibleUrns);
      // console.log('chartSelectedUrns: primaryMetricUrn', primaryMetricUrn);
      //
      const output = new Set(selectedUrns);
      output.add(primaryMetricUrn);
      invisibleUrns.forEach(urn => output.delete(urn));

      return output;
    }
  ),

  _timeseriesLoader: Ember.computed(
    'entities',
    function() {
      console.log('_timeseriesLoader()');
      const entities = this.get('entities');
      console.log('_timeseriesLoader: entities', entities);
      const metricUrns = Object.keys(entities).filter(urn => entities[urn] && entities[urn].type == 'metric');

      console.log('_timeseriesLoader: metricUrns', metricUrns);
      return this._startRequestMissingTimeseries(metricUrns); // current state, without new metrics
    }
  ),

  _entitiesLoader: Ember.computed(
    'model.eventEntities',
    'model.dimensionEntities',
    'model.metricEntities',
    function () {
      console.log('_entitiesLoader()');
      const eventEntities = this.get('model.eventEntities') || [];
      const dimensionEntities = this.get('model.dimensionEntities') || [];
      const metricEntities = this.get('model.metricEntities') || [];

      // console.log('_entitiesLoader: eventEntities', eventEntities);
      // console.log('_entitiesLoader: dimensionEntities', dimensionEntities);
      // console.log('_entitiesLoader: metricEntities', metricEntities);
      //
      const entities = {};
      eventEntities.forEach(e => entities[e.urn] = e);
      dimensionEntities.forEach(e => entities[e.urn] = e);
      metricEntities.forEach(e => entities[e.urn] = e);

      console.log('_entitiesLoader: merging cache');
      return this._updateEntitiesCache(entities);
    }
  ),

  _updateEntitiesCache(incoming) {
    console.log('_updateEntitiesCache()');
    const cache = this.get('_entitiesCache') || {};
    Object.keys(incoming).forEach(urn => cache[urn] = incoming[urn]);
    this.set('_entitiesCache', cache);
    return cache;
  },

  _updateTimeseriesCache(incoming) {
    console.log('_updateTimeseriesCache()');
    const cache = this.get('_timeseriesCache') || {};
    Object.keys(incoming).forEach(urn => cache[urn] = incoming[urn]);
    this.set('_timeseriesCache', cache);
    return cache;
  },

  _startRequestMissingTimeseries(urns) {
    console.log('_startRequestMissingTimeseries()');
    const pending = this.get('_pendingRequests') || new Set();
    const cache = this.get('_timeseriesCache') || {};

    const missing = new Set(urns);
    Object.keys(pending).forEach(missing.delete);
    Object.keys(cache).forEach(missing.delete);

    console.log('_startRequestMissingTimeseries: missing', missing);
    if (missing.size <= 0) {
      return cache;
    }

    const metricIds = [...missing].map(urn => urn.split(":")[2]);

    // NOTE: potential race condition?
    missing.forEach(urn => pending.add(urn));
    this.set('_pendingRequests', pending);

    const idString = metricIds.join(',');
    const analysisRange = this.get('model.analysisRange');

    const url = `/timeseries/query?metricIds=${idString}&ranges=${analysisRange[0]}:${analysisRange[1]}&granularity=15_MINUTES&transformations=timestamp,relative`;

    fetch(url)
      .then(res => res.json())
      .then(extractTimeseries)
      .then(incoming => this._completeRequestMissingTimeseries(this, incoming));

    return cache; // return current state, without new metrics
  },

  _completeRequestMissingTimeseries(that, incoming) {
    console.log('_completeRequestMissingTimeseries()');
    const pending = that.get('_pendingRequests') || new Set();

    // NOTE: potential race condition?
    Object.keys(incoming).forEach(urn => pending.delete(urn));
    that.set('_pendingRequests', pending);

    console.log('_completeRequestMissingTimeseries: merging cache');
    return that._updateTimeseriesCache(incoming); // return new state, including new metrics
  },

  actions: {
    addSelectedUrns(urns) {
      console.log('addSelectedUrns()');
      const selectedUrns = this.get('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.add(urn));
      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

    removeSelectedUrns(urns) {
      console.log('removeSelectedUrns()');
      const selectedUrns = this.get('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.delete(urn));
      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

    addFilteredUrns(urns) {
      console.log('addFilteredUrns()');
      const filteredUrns = this.get('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.add(urn));
      this.set('filteredUrns', filteredUrns);
      this.notifyPropertyChange('filteredUrns');
    },

    removeFilteredUrns(urns) {
      console.log('removeFilteredUrns()');
      const filteredUrns = this.get('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.delete(urn));
      this.set('filteredUrns', filteredUrns);
      this.notifyPropertyChange('filteredUrns');
    },

    addInvisibleUrns(urns) {
      console.log('addInvisibleUrns()');
      const invisibleUrns = this.get('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.add(urn));
      this.set('invisibleUrns', invisibleUrns);
      this.notifyPropertyChange('invisibleUrns');
    },

    removeInvisibleUrns(urns) {
      console.log('removeInvisibleUrns()');
      const invisibleUrns = this.get('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.delete(urn));
      this.set('invisibleUrns', invisibleUrns);
      this.notifyPropertyChange('invisibleUrns');
    }
  }
});
