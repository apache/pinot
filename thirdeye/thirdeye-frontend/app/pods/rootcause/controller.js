import Ember from 'ember';
import _ from 'lodash';

//
// Config
//

const EVENT_TABLE_COLUMNS = [
  {
    template: 'custom/checkbox',
    useFilter: false,
    mayBeHidden: false,
    className: 'events-table__column--checkbox'
  },
  {
    propertyName: 'label',
    title: 'Event Name',
    className: 'events-table__column'
  },
  {
    propertyName: 'eventType',
    title: 'Type',
    filterWithSelect: true,
    sortFilterOptions: true,
    className: 'events-table__column--compact'
  },
  {
    propertyName: 'start',
    title: 'Start',
    className: 'events-table__column--compact',
    disableFiltering: true
  },
  {
    propertyName: 'end',
    title: 'End',
    className: 'events-table__column--compact',
    disableFiltering: true
  }
];

//
// Controller
//

export default Ember.Controller.extend({
  selectedUrns: null, // Set

  filteredUrns: null, // Set

  invisibleUrns: null, // Set

  hoverUrns: null, // Set

  anomalyRange: null, // [2]

  baselineRange: null, // [2]

  analysisRange: null, // [2]

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

  eventTableEntities: Ember.computed(
    'entities',
    'filteredUrns',
    function () {
      console.log('eventTableEntities()');
      const entities = this.get('entities') || {};
      const filteredUrns = this.get('filteredUrns');
      return filterEntities(entities, (e) => filteredUrns.has(e.urn));
    }
  ),

  eventTableColumns: EVENT_TABLE_COLUMNS,

  eventFilterEntities: Ember.computed(
    'entities',
    function () {
      console.log('eventFilterEntities()');
      const entities = this.get('entities') || {};
      return filterEntities(entities, (e) => e.type == 'event');
    }
  ),

  tooltipEntities: Ember.computed(
    'entities',
    'invisibleUrns',
    'hoverUrns',
    function () {
      const entities = this.get('entities') || {};
      const invisibleUrns = this.get('invisibleUrns');
      const hoverUrns = this.get('hoverUrns');

      const visibleUrns = makeIterable(hoverUrns).filter(urn => !invisibleUrns.has(urn));

      return filterEntities(entities, (e) => visibleUrns.has(e.urn));
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
    toggleInvisible(urn) {
      const invisibleUrns = this.get('invisibleUrns');
      if (invisibleUrns.has(urn)) {
        invisibleUrns.delete(urn);
      } else {
        invisibleUrns.add(urn);
      }
      this.set('invisibleUrns', invisibleUrns);
      this.notifyPropertyChange('invisibleUrns');
    },

    tableOnSelect(tableUrns) {
      console.log('tableOnSelect()');
      const entities = this.get('entities');
      const filteredUrns = this.get('filteredUrns');
      const selectedUrns = this.get('selectedUrns');

      const tableEventUrns = new Set(tableUrns);
      const selectedEventUrns = new Set(makeIterable(selectedUrns).filter(urn => entities[urn] && entities[urn].type == 'event'));
      console.log('tableOnSelect: tableEventUrns', tableEventUrns);
      console.log('tableOnSelect: selectedEventUrns', selectedEventUrns);

      makeIterable(selectedEventUrns).filter(urn => filteredUrns.has(urn) && !tableEventUrns.has(urn)).forEach(urn => selectedUrns.delete(urn));
      makeIterable(tableEventUrns).forEach(urn => selectedUrns.add(urn));

      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

    filterOnSelect(urns) {
      console.log('filterOnSelect()');
      this.set('filteredUrns', new Set(urns));
      this.notifyPropertyChange('filteredUrns');
    },

    chartOnHover(urns) {
      console.log('chartOnHover()');
      console.log('chartOnHover: urns', urns);
      this.set('hoverUrns', new Set(urns));
      this.notifyPropertyChange('hoverUrns');
    },

    loadtestSelectedUrns() {
      console.log('loadtestSelected()');
      const entities = this.get('entities');
      this.set('selectedUrns', new Set(Object.keys(entities)));
      this.notifyPropertyChange('selectedUrns');
    },

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

//
// Helpers
//

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

const filterEntities = (obj, func) => {
  const out = {};
  Object.keys(obj).filter(key => func(obj[key])).forEach(key => out[key] = obj[key]);
  return out;
};
