import Ember from 'ember';
import { makeIterable, filterObject } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import fetch from 'fetch';

//
// Controller
//

export default Ember.Controller.extend({
  selectedUrns: null, // Set

  filteredUrns: null, // Set

  invisibleUrns: null, // Set

  hoverUrns: null, // Set

  context: null, // { urns: Set, anomalyRange: [2], baselineRange: [2], analysisRange: [2] }

  _timeseriesCache: null, // {}

  _entitiesCache: null, // {}

  _pendingTimeseriesRequests: null, // Set

  _pendingEntitiesRequests: null, // Set

  init() {
    this.setProperties({ _timeseriesCache: {}, _entitiesCache: {},
      _pendingTimeseriesRequests: new Set(), _pendingEntitiesRequests: new Set() });
  },

  //
  // Public properties (computed)
  //

  entities: Ember.computed(
    '_entitiesLoader',
    '_entitiesCache',
    function () {
      console.log('entities()');
      this.get('_entitiesLoader'); // trigger loader. hacky
      return Object.assign({}, this.get('_entitiesCache'));
    }
  ),

  timeseries: Ember.computed(
    '_timeseriesLoader',
    '_timeseriesCache',
    function () {
      console.log('timeseries()');
      this.get('_timeseriesLoader'); // trigger loader. hacky
      return Object.assign({}, this.get('_timeseriesCache'));
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

      const selectedBaselineUrns = [...selectedMetricUrns].filter(urn => entities[urn] && entities[urn].type == 'metric').map(urn => this._makeMetricBaselineUrn(urn));

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

  isLoadingEntities: Ember.computed(
    'entities',
    '_pendingEntitiesRequests',
    function () {
      this.get('entities'); // force entities computation first
      return this.get('_pendingEntitiesRequests').size > 0;
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
  // Entities loading
  //

  _entitiesLoader: Ember.computed(
    'context',
    'anomalyRegion',
    'baselineRegion',
    'analysisRegion',
    function () {
      console.log('_entitiesLoader()');
      const { cache } = this.getProperties('_entitiesCache');
      this._startRequestEntities();
      return cache;
    }
  ),

  _startRequestEntities() {
    console.log('_startRequestEntities()');
    const { context, _pendingEntitiesRequests: pending } =
      this.getProperties('context', '_pendingEntitiesRequests');

    const frameworks = new Set(['relatedEvents', 'relatedDimensions', 'relatedMetrics']);

    // TODO prevent skipping overlapping changes to context
    [...pending].forEach(framework => frameworks.delete(framework));

    if (frameworks.size <= 0) {
      return;
    }

    [...frameworks].forEach(framework => pending.add(framework));

    this.setProperties({ _pendingEntitiesRequests: pending });
    this.notifyPropertyChange('_pendingEntitiesRequests');

    frameworks.forEach(framework => {
      const url = this._makeFrameworkUrl(framework, context);
      fetch(url)
        .then(res => res.json())
        .then(res => this._resultToEntities(res))
        .then(json => this._completeRequestEntities(this, json, framework));
    });

  },

  _completeRequestEntities(that, incoming, framework) {
    console.log('_completeRequestEntities()');
    const { selectedUrns, _pendingEntitiesRequests: pending, _entitiesCache: entitiesCache, _timeseriesCache: timeseriesCache } =
      this.getProperties('selectedUrns', '_pendingEntitiesRequests', '_entitiesCache', '_timeseriesCache');

    // update pending requests
    pending.delete(framework);

    // timeseries eviction
    // TODO optimize for same time range reload
    Object.keys(incoming).forEach(urn => delete timeseriesCache[urn]);
    Object.keys(incoming).forEach(urn => delete timeseriesCache[that._makeMetricBaselineUrn(urn)]);

    // entities eviction
    const candidates = that._entitiesEvictionUrns(entitiesCache, framework);
    [...candidates].filter(urn => !selectedUrns.has(urn)).forEach(urn => delete entitiesCache[urn]);

    // augmentation
    const augmented = Object.assign({}, incoming, that._entitiesMetricsAugmentation(incoming));

    // update entities cache
    Object.keys(augmented).forEach(urn => entitiesCache[urn] = augmented[urn]);

    that.setProperties({ _entitiesCache: entitiesCache, _timeseriesCache: timeseriesCache, _pendingEntitiesRequests: pending });
    that.notifyPropertyChange('_timeseriesCache');
    that.notifyPropertyChange('_entitiesCache');
    that.notifyPropertyChange('_pendingEntitiesRequests');
  },

  _entitiesEvictionUrns(cache, framework) {
    if (framework == 'relatedEvents') {
      return new Set(Object.keys(cache).filter(urn => cache[urn].type == 'event'));
    }
    if (framework == 'relatedDimensions') {
      return new Set(Object.keys(cache).filter(urn => cache[urn].type == 'dimension'));
    }
    if (framework == 'relatedMetrics') {
      return new Set(Object.keys(cache).filter(urn => cache[urn].type == 'metric'));
    }
  },

  _entitiesMetricsAugmentation(incoming) {
    console.log('_entitiesMetricsAugmentation()');
    const entities = {};
    Object.keys(incoming).filter(urn => incoming[urn].type == 'metric').forEach(urn => {
      const baselineUrn = this._makeMetricBaselineUrn(urn);
      entities[baselineUrn] = {
        urn: baselineUrn,
        type: 'frontend:baseline:metric',
        label: incoming[urn].label + ' (baseline)'
      };
    });
    return entities;
  },

  _makeFrameworkUrl(framework, context) {
    const urnString = [...context.urns].join(',');
    return `/rootcause/query?framework=${framework}` +
      `&anomalyStart=${context.anomalyRange[0]}&anomalyEnd=${context.anomalyRange[1]}` +
      `&baselineStart=${context.baselineRange[0]}&baselineEnd=${context.baselineRange[1]}` +
      `&analysisStart=${context.analysisRange[0]}&analysisEnd=${context.analysisRange[1]}` +
      `&urns=${urnString}`;
  },

  _resultToEntities(res) {
    const entities = {};
    res.forEach(e => entities[e.urn] = e);
    return entities;
  },

  //
  // Timeseries loading
  //

  _timeseriesLoader: Ember.computed(
    'entities',
    function() {
      console.log('_timeseriesLoader()');
      const { entities, cache } = this.getProperties('entities', '_timeseriesCache');

      const metricUrns = Object.keys(entities).filter(urn => entities[urn] && entities[urn].type == 'metric');
      const baselineUrns = metricUrns.map(urn => this._makeMetricBaselineUrn(urn));

      this._startRequestMissingTimeseries(metricUrns.concat(baselineUrns));

      return cache;
    }
  ),

  _startRequestMissingTimeseries(urns) {
    console.log('_startRequestMissingTimeseries()');
    const { _pendingTimeseriesRequests: pending, _timeseriesCache: cache, context } =
      this.getProperties('_pendingTimeseriesRequests', '_timeseriesCache', 'context');

    const missing = new Set(urns);
    [...pending].forEach(urn => missing.delete(urn));
    Object.keys(cache).forEach(urn => missing.delete(urn));

    if (missing.size <= 0) {
      return;
    }

    [...missing].forEach(urn => pending.add(urn));

    this.setProperties({_pendingTimeseriesRequests: pending});
    this.notifyPropertyChange('_pendingTimeseriesRequests');

    // metrics
    const metricUrns = [...missing].filter(urn => urn.startsWith('thirdeye:metric:'));
    const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
    const metricUrl = `/timeseries/query?metricIds=${metricIdString}&ranges=${context.analysisRange[0]}:${context.analysisRange[1]}&granularity=15_MINUTES&transformations=timestamp`;

    fetch(metricUrl)
      .then(res => res.json())
      .then(this._extractTimeseries)
      .then(timeseries => this._completeRequestMissingTimeseries(this, timeseries));

    // baselines
    const baselineOffset = context.anomalyRange[0] - context.baselineRange[0];
    const baselineDisplayStart = context.analysisRange[0] - baselineOffset;
    const baselineDisplayEnd = context.analysisRange[1] - baselineOffset;

    const baselineUrns = [...missing].filter(urn => urn.startsWith('frontend:baseline:metric:'));
    const baselineIdString = baselineUrns.map(urn => urn.split(":")[3]).join(',');
    const baselineUrl = `/timeseries/query?metricIds=${baselineIdString}&ranges=${baselineDisplayStart}:${baselineDisplayEnd}&granularity=15_MINUTES&transformations=timestamp`;

    fetch(baselineUrl)
      .then(res => res.json())
      .then(res => this._extractTimeseries(res))
      .then(timeseries => this._convertMetricToBaseline(timeseries, baselineOffset))
      .then(timeseries => this._completeRequestMissingTimeseries(this, timeseries));

  },

  _completeRequestMissingTimeseries(that, incoming) {
    console.log('_completeRequestMissingTimeseries()');
    const { _pendingTimeseriesRequests: pending, _timeseriesCache: cache } = that.getProperties('_pendingTimeseriesRequests', '_timeseriesCache');

    Object.keys(incoming).forEach(urn => pending.delete(urn));
    Object.keys(incoming).forEach(urn => cache[urn] = incoming[urn]);

    that.setProperties({ _pendingTimeseriesRequests: pending, _timeseriesCache: cache });
    that.notifyPropertyChange('_timeseriesCache');
    that.notifyPropertyChange('_pendingTimeseriesRequests');
  },

  _extractTimeseries(json) {
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
  },

  _convertMetricToBaseline(timeseries, offset) {
    const baseline = {};
    Object.keys(timeseries).forEach(urn => {
      const baselineUrn = this._makeMetricBaselineUrn(urn);
      baseline[baselineUrn] = {
        values: timeseries[urn].values,
        timestamps: timeseries[urn].timestamps.map(t => t + offset)
      };
    });
    return baseline;
  },

  _makeMetricBaselineUrn(urn) {
    const mid = urn.split(':')[2];
    return `frontend:baseline:metric:${mid}`;
  },

  //
  // Actions
  //

  actions: {
    toggleInvisible(urn) {
      const { invisibleUrns } = this.getProperties('invisibleUrns');
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
      const { entities, filteredUrns, selectedUrns } =
        this.getProperties('entities', 'filteredUrns', 'selectedUrns');

      const tableEventUrns = new Set(tableUrns);
      const selectedEventUrns = new Set(makeIterable(selectedUrns).filter(urn => entities[urn] && entities[urn].type == 'event'));

      makeIterable(selectedEventUrns).filter(urn => filteredUrns.has(urn) && !tableEventUrns.has(urn)).forEach(urn => selectedUrns.delete(urn));
      makeIterable(tableEventUrns).forEach(urn => selectedUrns.add(urn));

      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

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
      console.log('settingsOnChange: context', context);
      this.set('context', context);
    },

    addSelectedUrns(urns) {
      console.log('addSelectedUrns()');
      const { selectedUrns } = this.getProperties('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.add(urn));
      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

    removeSelectedUrns(urns) {
      console.log('removeSelectedUrns()');
      const { selectedUrns } = this.getProperties('selectedUrns');
      makeIterable(urns).forEach(urn => selectedUrns.delete(urn));
      this.set('selectedUrns', selectedUrns);
      this.notifyPropertyChange('selectedUrns');
    },

    addFilteredUrns(urns) {
      console.log('addFilteredUrns()');
      const { filteredUrns } = this.getProperties('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.add(urn));
      this.set('filteredUrns', filteredUrns);
      this.notifyPropertyChange('filteredUrns');
    },

    removeFilteredUrns(urns) {
      console.log('removeFilteredUrns()');
      const { filteredUrns } = this.getProperties('filteredUrns');
      makeIterable(urns).forEach(urn => filteredUrns.delete(urn));
      this.set('filteredUrns', filteredUrns);
      this.notifyPropertyChange('filteredUrns');
    },

    addInvisibleUrns(urns) {
      console.log('addInvisibleUrns()');
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.add(urn));
      this.set('invisibleUrns', invisibleUrns);
      this.notifyPropertyChange('invisibleUrns');
    },

    removeInvisibleUrns(urns) {
      console.log('removeInvisibleUrns()');
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      makeIterable(urns).forEach(urn => invisibleUrns.delete(urn));
      this.set('invisibleUrns', invisibleUrns);
      this.notifyPropertyChange('invisibleUrns');
    }
  }
});

