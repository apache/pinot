import Ember from 'ember';

const extractTimeseries = (json) => {
  const timeseries = {};

  Object.keys(json).forEach(range =>
    Object.keys(json[range]).filter(sid => sid != 'timestamp').forEach(sid => {
      const urn = `thirdeye:metric:${sid}`;
      timeseries[urn] = {
        timestamps: json[range].timestamp,
        values: json[range][sid]
      };
    })
  );

  return timeseries;
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
      this.get('_entitiesLoader');
      return this.get('_entitiesCache') || {};
    }
  ),

  timeseries: Ember.computed(
    '_timeseriesLoader',
    '_timeseriesCache',
    function () {
      console.log('timeseries()');
      this.get('_timeseriesLoader');
      return this.get('_timeseriesCache') || {};
    }
  ),

  chartSelectedUrns: Ember.computed(
    'selectedUrns',
    'model.primaryMetricUrn',
    'invisibleUrns',
    function () {
      console.log('chartSelectedUrns()');
      const selectedUrns = this.get('selectedUrns');
      const invisibleUrns = this.get('invisibleUrns');
      const primaryMetricUrn = this.get('model.primaryMetricUrn');

      console.log('chartSelectedUrns: selectedUrns', selectedUrns);
      console.log('chartSelectedUrns: invisibleUrns', invisibleUrns);
      console.log('chartSelectedUrns: primaryMetricUrn', primaryMetricUrn);

      const output = new Set(selectedUrns);
      output.add(primaryMetricUrn);
      invisibleUrns.forEach(output.delete);

      return output;
    }
  ),

  _timeseriesLoader: Ember.computed(
    'entities',
    function() {
      console.log('_timeseriesLoader()');
      const entities = this.get('entities');
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

      console.log('_entitiesLoader: eventEntities', eventEntities);
      console.log('_entitiesLoader: dimensionEntities', dimensionEntities);
      console.log('_entitiesLoader: metricEntities', metricEntities);

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
    const updatedCache = Object.assign({}, cache, incoming);
    this.set('_entitiesCache', updatedCache);
    return updatedCache;
  },

  _updateTimeseriesCache(incoming) {
    console.log('_updateTimeseriesCache()');
    const cache = this.get('_timeseriesCache') || {};
    const updatedCache = Object.assign({}, cache, incoming);
    this.set('_timeseriesCache', updatedCache);
    return updatedCache;
  },

  _startRequestMissingTimeseries(urns) {
    console.log('_startRequestMissingTimeseries()');
    const pending = this.get('_pendingRequests') || {};
    const cache = this.get('_timeseriesCache') || {};

    const missing = new Set(urns);
    Object.keys(pending).forEach(missing.delete);
    Object.keys(cache).forEach(missing.delete);

    console.log('_startRequestMissingTimeseries: missing', missing);
    if (missing.size <= 0) {
      return cache;
    }

    const metricIds = [...missing].map(urn => urn.split(":")[2]);
    const metricUrns = metricIds.map(mid => `thirdeye:metric:${mid}`);

    const updatedPending = Object.assign({}, updatedPending, metricUrns);

    // NOTE: potential race condition?
    this.set('_pendingRequests', updatedPending);

    const idString = metricIds.join(',');
    const analysisRange = this.get('model.analysisRange');

    const url = `/timeseries/query?metricIds=${idString}&ranges=${analysisRange[0]}:${analysisRange[1]}&granularity=5_MINUTES&transformations=timestamp,fillforward`;

    fetch(url)
      .then(res => res.json())
      .then(extractTimeseries)
      .then(incoming => this._completeRequestMissingTimeseries(this, incoming));

    return cache; // return current state, without new metrics
  },

  _completeRequestMissingTimeseries(that, incoming) {
    console.log('_completeRequestMissingTimeseries()');
    const pending = that.get('_pendingRequests') || {};

    const updatedPending = Object.assign({}, pending);
    Object.keys(incoming).forEach(urn => delete updatedPending[urn]);

    // NOTE: potential race condition?
    that.set('_pendingRequests', updatedPending);

    console.log('_completeRequestMissingTimeseries: merging cache');
    return that._updateTimeseriesCache(incoming); // return new state, including new metrics
  },

  actions: {
    addSelectUrns(urns) {
      const selectedUrns = this.get('selectedUrns');
      urns.forEach(selectedUrns.add);
      this.set('selectedUrns', Object.assign({}, selectedUrns));
    },

    removeSelectUrns(urns) {
      const selectedUrns = this.get('selectedUrns');
      urns.forEach(selectedUrns.delete);
      this.set('selectedUrns', Object.assign({}, selectedUrns));
    },

    addFilteredUrns(urns) {
      const filteredUrns = this.get('filteredUrns');
      urns.forEach(filteredUrns.add);
      this.set('filteredUrns', Object.assign({}, filteredUrns));
    },

    removeFilteredUrns(urns) {
      const filteredUrns = this.get('filteredUrns');
      urns.forEach(filteredUrns.delete);
      this.set('filteredUrns', Object.assign({}, filteredUrns));
    },

    addInvisibleUrns(urns) {
      const invisibleUrns = this.get('invisibleUrns');
      urns.forEach(invisibleUrns.add);
      this.set('invisibleUrns', Object.assign({}, invisibleUrns));
    },

    removeInvisibleUrns(urns) {
      const invisibleUrns = this.get('invisibleUrns');
      urns.forEach(invisibleUrns.delete);
      this.set('filteredUrns', Object.assign({}, invisibleUrns));
    }
  }
});
