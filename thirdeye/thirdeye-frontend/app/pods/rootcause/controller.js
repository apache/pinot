import Ember from 'ember';
import { checkStatus, makeIterable, filterObject } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import fetch from 'fetch';
import config from 'thirdeye-frontend/mocks/filterBarConfig';

//
// Controller
//

export default Ember.Controller.extend({
  entitiesService: Ember.inject.service("rootcause-entities-cache"), // service

  timeseriesService: Ember.inject.service("rootcause-timeseries-cache"), // service

  selectedUrns: null, // Set

  filteredUrns: null, // Set

  invisibleUrns: null, // Set

  hoverUrns: null, // Set

  context: null, // { urns: Set, anomalyRange: [2], baselineRange: [2], analysisRange: [2] }

  config: config, // {}

  _timeseriesCache: null, // {}

  _entitiesCache: null, // {}

  _aggregatesCache: null, // {}

  _pendingTimeseriesRequests: null, // Set

  _pendingEntitiesRequests: null, // Set

  _pendingAggregatesRequests: null, // Set

  _contextObserver: Ember.observer(
    'context',
    'selectedUrns',
    'entitiesService',
    'timeseriesService',
    function () {
      console.log('_contextObserver()');
      const { context, selectedUrns, entitiesService, timeseriesService } =
        this.getProperties('context', 'selectedUrns', 'entitiesService', 'timeseriesService');
      if (!context || !selectedUrns) {
        return;
      }

      if (entitiesService) {
        entitiesService.request(context, selectedUrns);
      }

      if (timeseriesService) {
        timeseriesService.request(context, selectedUrns);
      }
    }
  ),

  init() {
    console.log('controller: init()');
    this.setProperties({ _timeseriesCache: {}, _entitiesCache: {}, _aggregatesCache: {},
      _pendingTimeseriesRequests: new Set(), _pendingEntitiesRequests: new Set(), _pendingAggregatesRequests: new Set() });
  },

  //
  // Public properties (computed)
  //

  entities: Ember.computed(
    'entitiesService.entities',
    function () {
      console.log('entities()');
      return this.get('entitiesService.entities');
    }
  ),

  timeseries: Ember.computed(
    // '_timeseriesLoader',
    // '_timeseriesCache',
    'timeseriesService.timeseries',
    function () {
      console.log('timeseries()');
      // this.get('_timeseriesLoader'); // trigger loader. hacky
      return this.get('timeseriesService.timeseries');
      // return Object.assign({}, this.get('_timeseriesCache'));
    }
  ),

  aggregates: Ember.computed(
    '_aggregatesLoader',
    '_aggregatesCache',
    function () {
      console.log('aggregates()');
      this.get('_aggregatesLoader'); // trigger loader. hacky
      return Object.assign({}, this.get('_aggregatesCache'));
    }
  ),

  chartSelectedUrns: Ember.computed(
    'entities',
    'context',
    'selectedUrns',
    'invisibleUrns',
    function () {
      console.log('chartSelectedUrns()');
      const { entities, selectedUrns, invisibleUrns, context } =
        this.getProperties('entities', 'selectedUrns', 'invisibleUrns', 'context');

      const selectedMetricUrns = new Set(selectedUrns);
      [...invisibleUrns].forEach(urn => selectedMetricUrns.delete(urn));
      [...context.urns].filter(urn => entities[urn] && entities[urn].type == 'metric').forEach(urn => selectedMetricUrns.add(urn));

      const selectedBaselineUrns = [...selectedMetricUrns].filter(urn => entities[urn] && entities[urn].type == 'metric').map(urn => this._makeBaselineUrn(urn));

      return new Set([...selectedMetricUrns].concat(selectedBaselineUrns));
    }
  ),

  eventTableEntities: Ember.computed(
    'entities',
    'filteredUrns',
    function () {
      console.log('eventTableEntities()');
      const { entities, filteredUrns } = this.getProperties('entities', 'filteredUrns');
      return filterObject(entities, (e) => filteredUrns.has(e.urn));
    }
  ),

  eventTableColumns: EVENT_TABLE_COLUMNS,

  eventFilterEntities: Ember.computed(
    'entities',
    function () {
      console.log('eventFilterEntities()');
      const { entities } = this.getProperties('entities');
      console.log("printing entities: ", entities);
      return filterObject(entities, (e) => e.type == 'event');
    }
  ),

  tooltipEntities: Ember.computed(
    'entities',
    'invisibleUrns',
    'hoverUrns',
    function () {
      const { entities, invisibleUrns, hoverUrns } = this.getProperties('entities', 'invisibleUrns', 'hoverUrns');
      const visibleUrns = [...hoverUrns].filter(urn => !invisibleUrns.has(urn));
      return filterObject(entities, (e) => visibleUrns.has(e.urn));
    }
  ),

  heatmapCurrentUrns: Ember.computed(
    'context',
    function () {
      const { context } = this.getProperties('context');
      return new Set([...context.urns].filter(urn => urn.startsWith('thirdeye:metric:')));
    }
  ),

  heatmapCurrent2Baseline: Ember.computed(
    'context',
    function () {
      const { heatmapCurrentUrns } = this.getProperties('heatmapCurrentUrns');
      const current2baseline = {};
      [...heatmapCurrentUrns].forEach(urn => current2baseline[urn] = this._makeBaselineUrn(urn));
      return current2baseline;
    }
  ),

  metricEntities: Ember.computed(
    'entities',
    function () {
      console.log('metricEntities()');
      const { entities } = this.getProperties('entities');
      return filterObject(entities, (e) => e.type == 'metric');
    }
  ),

  isLoadingEntities: Ember.computed(
    'entitiesService.entities',
    function () {
      console.log('isLoadingEntities()');
      return this.get('entitiesService.pending').size > 0;
    }
  ),

  isLoadingTimeseries: Ember.computed(
    'timeseries',
    '_pendingTimeseriesRequests',
    function () {
      this.get('timeseries'); // force timeseries computation first
      return this.get('_pendingTimeseriesRequests').size > 0;
    }
  ),

  //
  // Aggregates loading
  //

  _aggregatesLoader: Ember.computed(
    'entities',
    'context',
    function() {
      console.log('_aggregatesLoader()');
      const { context } = this.getProperties('context');

      const currentUrns = [...context.urns].filter(urn => urn.startsWith('thirdeye:metric:'));
      if (currentUrns.length <= 0) {
        return;
      }
      const baselineUrns = currentUrns.map(urn => this._makeBaselineUrn(urn));

      this._startRequestMissingAggregates(currentUrns.concat(baselineUrns));
    }
  ),

  _startRequestMissingAggregates(urns) {
    console.log('_startRequestMissingAggregates()');
    const { _pendingAggregatesRequests: pending, _aggregatesCache: cache, context } =
      this.getProperties('_pendingAggregatesRequests', '_aggregatesCache', 'context');

    const missing = new Set(urns);
    [...pending].forEach(urn => missing.delete(urn));
    Object.keys(cache).forEach(urn => missing.delete(urn));

    if (missing.size <= 0) {
      return;
    }

    [...missing].forEach(urn => pending.add(urn));

    this.setProperties({_pendingAggregatesRequests: pending});
    this.notifyPropertyChange('_pendingAggregatesRequests');

    // currents
    const metricUrns = [...missing].filter(urn => urn.startsWith('thirdeye:metric:'));
    const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
    const metricUrl = `/aggregation/query?metricIds=${metricIdString}&ranges=${context.anomalyRange[0]}:${context.anomalyRange[1]}&granularity=15_MINUTES&transformations=timestamp`;

    fetch(metricUrl)
      .then(checkStatus)
      .then(res => this._extractAggregates(res, (mid) => `thirdeye:metric:${mid}`))
      .then(aggregates => this._completeRequestMissingAggregates(aggregates));

    // baseline
    const baselineUrns = [...missing].filter(urn => urn.startsWith('frontend:baseline:metric:'));
    const baselineIdString = baselineUrns.map(urn => urn.split(":")[3]).join(',');
    const baselineUrl = `/aggregation/query?metricIds=${baselineIdString}&ranges=${context.baselineRange[0]}:${context.baselineRange[1]}&granularity=15_MINUTES&transformations=timestamp`;

    fetch(baselineUrl)
      .then(checkStatus)
      .then(res => this._extractAggregates(res, (mid) => `frontend:baseline:metric:${mid}`))
      .then(aggregates => this._completeRequestMissingAggregates(aggregates));

  },

  _completeRequestMissingAggregates(incoming) {
    console.log('_completeRequestMissingAggregates()');
    const { _pendingAggregatesRequests: pending, _aggregatesCache: cache } =
      this.getProperties('_pendingAggregatesRequests', '_aggregatesCache');

    Object.keys(incoming).forEach(urn => pending.delete(urn));
    Object.keys(incoming).forEach(urn => cache[urn] = incoming[urn]);

    this.setProperties({ _pendingAggregatesRequests: pending, _aggregatesCache: cache });
    this.notifyPropertyChange('_aggregatesCache');
    this.notifyPropertyChange('_pendingAggregatesRequests');
  },

  _extractAggregates(incoming, urnFunc) {
    // NOTE: only supports single time range
    const aggregates = {};
    Object.keys(incoming).forEach(range => {
      Object.keys(incoming[range]).forEach(mid => {
        const aggregate = incoming[range][mid];
        const urn = urnFunc(mid);
        aggregates[urn] = aggregate;
      });
    });
    return aggregates;
  },

  _makeBaselineUrn(urn) {
    const mid = urn.split(':')[2];
    return `frontend:baseline:metric:${mid}`;
  },

  //
  // Actions
  //

  actions: {
    onSelection(updates) {
      console.log('onSelection()');
      console.log('onSelection: updates', updates);
      const { selectedUrns } = this.getProperties('selectedUrns');
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => selectedUrns.add(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => selectedUrns.delete(urn));
      this.set('selectedUrns', new Set(selectedUrns));
    },

    onVisibility(updates) {
      console.log('onVisibility()');
      console.log('onVisibility: updates', updates);
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => invisibleUrns.delete(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => invisibleUrns.add(urn));
      this.set('invisibleUrns', new Set(invisibleUrns));
    },

    // TODO refactor filter to match onSelection()
    filterOnSelect(urns) {
      console.log('filterOnSelect()');
      this.set('filteredUrns', new Set(urns));
    },

    chartOnHover(urns) {
      console.log('chartOnHover()');
      this.set('hoverUrns', new Set(urns));
    },

    loadtestSelectedUrns() {
      console.log('loadtestSelected()');
      const { entities } = this.getProperties('entities');
      this.set('selectedUrns', new Set(Object.keys(entities)));
    },

    settingsOnChange(context) {
      console.log('settingsOnChange()');
      this.set('context', context);
    },

    addSelectedUrns(urns) {
      console.log('addSelectedUrns()');
      const { selectedUrns } = this.getProperties('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.add(urn));
      this.set('selectedUrns', new Set(selectedUrns));
    },

    removeSelectedUrns(urns) {
      console.log('removeSelectedUrns()');
      const { selectedUrns } = this.getProperties('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.delete(urn));
      this.set('selectedUrns', new Set(selectedUrns));
    },

    addFilteredUrns(urns) {
      console.log('addFilteredUrns()');
      const { filteredUrns } = this.getProperties('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.add(urn));
      this.set('filteredUrns', new Set(filteredUrns));
    },

    removeFilteredUrns(urns) {
      console.log('removeFilteredUrns()');
      const { filteredUrns } = this.getProperties('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.delete(urn));
      this.set('filteredUrns', new Set(filteredUrns));
    },

    addInvisibleUrns(urns) {
      console.log('addInvisibleUrns()');
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.add(urn));
      this.set('invisibleUrns', new Set(invisibleUrns));
    },

    removeInvisibleUrns(urns) {
      console.log('removeInvisibleUrns()');
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.delete(urn));
      this.set('invisibleUrns', new Set(invisibleUrns));
    }
  }
});

