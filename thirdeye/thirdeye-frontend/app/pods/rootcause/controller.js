import Ember from 'ember';
import { makeIterable, filterObject, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import config from 'thirdeye-frontend/mocks/filterBarConfig';
import moment from 'moment';

const {
  getProperties,
  get,
  setProperties,
  computed
} = Ember;

export default Ember.Controller.extend({

  /**
   * QueryParams that needs to update the url
   */
  queryParams: [
    'granularity',
    'filters',
    'compareMode',
    'anomalyRangeStart',
    'anomalyRangeEnd',
    'analysisRangeStart',
    'analysisRangeEnd'
  ],
  entitiesService: Ember.inject.service('rootcause-entities-cache'), // service

  timeseriesService: Ember.inject.service('rootcause-timeseries-cache'), // service

  aggregatesService: Ember.inject.service('rootcause-aggregates-cache'), // service

  breakdownsService: Ember.inject.service('rootcause-breakdowns-cache'), // service


  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  hoverUrns: null, // Set

  context: null, // { urns: Set, anomalyRange: [2], baselineRange: [2], analysisRange: [2] }

  config: config, // {}

  filteredUrns: null,

  _contextObserver: Ember.observer(
    'context',
    'entities',
    'selectedUrns',
    'entitiesService',
    'timeseriesService',
    'aggregatesService',
    'breakdownsService',
    function () {
      console.log('_contextObserver()');
      const { context, entities, selectedUrns, entitiesService, timeseriesService, aggregatesService, breakdownsService } =
        this.getProperties('context', 'entities', 'selectedUrns', 'entitiesService', 'timeseriesService', 'aggregatesService', 'breakdownsService');

      if (!context || !selectedUrns) {
        return;
      }

      entitiesService.request(context, selectedUrns);
      timeseriesService.request(context, selectedUrns);
      breakdownsService.request(context, selectedUrns);

      const allUrns = new Set(Object.keys(entities));
      aggregatesService.request(context, allUrns);
    }
  ),

  /**
   * Configuration for the Settings component
   */
  settingsConfig: computed(
    'model.{granularityOptions,filterOptions,compareModeOptions}',
    function() {
      const model = get(this, 'model');
      const settingsOptions = ['granularityOptions', 'filterOptions', 'compareModeOptions'];
      // debugger;
      return getProperties(model, settingsOptions);
    }
  ),

  //
  // Public properties (computed)
  //

  /**
   * Hash containing all possible option values
   * @type {Object}
   */
  options: Ember.computed('model', function() {
    const model = this.get('model');
    return getProperties(model, ['granularityOptions', 'filterOptions', 'maxTime', 'compareModeOptions']);
  }),

  /**
   * hash containing user selected values
   * @type {Object}
   */
  selectedOptions: Ember.computed(function() {
    const queryParams = get(this, 'queryParams');

    return queryParams.reduce((hash, param) => {
      const value = this.queryToOption(param, get(this, param));
      
      hash[param] = value;
      return hash;
    }, {});
  }),

  /**
   * Convert a  query value into option vlaue
   * @param {String} key          - query param key
   * @param {String|Object} value - query param value
   */
  queryToOption(key, value) {
    switch(key) {
      case 'baselineRangeStart':
      case 'baselineRangeEnd':
      case 'anomalyRangeStart':
      case 'anomalyRangeEnd':
      case 'analysisRangeStart':
      case 'analysisRangeEnd':
        return moment(+value).isValid ? parseInt(value) : undefined;
      case 'granularity':
      case 'filters':
      case 'compareMode':
        return value;
    }
  },

  entities: Ember.computed(
    'entitiesService.entities',
    function () {
      console.log('entities()');
      return this.get('entitiesService.entities');
    }
  ),

  timeseries: Ember.computed(
    'timeseriesService.timeseries',
    function () {
      console.log('timeseries()');
      return this.get('timeseriesService.timeseries');
    }
  ),

  aggregates: Ember.computed(
    'aggregatesService.aggregates',
    function () {
      console.log('aggregates()');
      return this.get('aggregatesService.aggregates');
    }
  ),

  breakdowns: Ember.computed(
    'breakdownsService.breakdowns',
    function () {
      console.log('breakdowns()');
      return this.get('breakdownsService.breakdowns');
    }
  ),

  chartSelectedUrns: Ember.computed(
    'entities',
    'selectedUrns',
    'invisibleUrns',
    function () {
      console.log('chartSelectedUrns()');
      const { selectedUrns, invisibleUrns } =
        this.getProperties('selectedUrns', 'invisibleUrns');

      const urns = new Set(selectedUrns);
      [...invisibleUrns].forEach(urn => urns.delete(urn));

      return urns;
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
    'selectedUrns',
    function () {
      const { selectedUrns } = this.getProperties('selectedUrns');
      return new Set(filterPrefix(selectedUrns, 'thirdeye:metric:'));
    }
  ),

  heatmapCurrent2Baseline: Ember.computed(
    'selectedUrns',
    function () {
      const { selectedUrns } = this.getProperties('selectedUrns');
      const baselineUrns = {};
      filterPrefix(selectedUrns, 'thirdeye:metric:').forEach(urn => baselineUrns[urn] = toBaselineUrn(urn));
      return baselineUrns;
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
    'timeseriesService.timeseries',
    function () {
      console.log('isLoadingTimeseries()');
      return this.get('timeseriesService.pending').size > 0;
    }
  ),

  isLoadingAggregates: Ember.computed(
    'aggregatesService.aggregates',
    function () {
      console.log('isLoadingAggregates()');
      return this.get('aggregatesService.aggregates').size > 0;
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
        .then(checkStatus)
        .then(this._resultToEntities)
        .then(json => this._completeRequestEntities(json, framework));
    });

  },

  _completeRequestEntities(incoming, framework) {
    console.log('_completeRequestEntities()');
    const { selectedUrns, _pendingEntitiesRequests: pending, _entitiesCache: entitiesCache, _timeseriesCache: timeseriesCache } =
      this.getProperties('selectedUrns', '_pendingEntitiesRequests', '_entitiesCache', '_timeseriesCache');

    // update pending requests
    pending.delete(framework);

    // timeseries eviction
    // TODO optimize for same time range reload
    Object.keys(incoming).forEach(urn => delete timeseriesCache[urn]);
    Object.keys(incoming).forEach(urn => delete timeseriesCache[this._makeMetricBaselineUrn(urn)]);

    // entities eviction
    const candidates = this._entitiesEvictionUrns(entitiesCache, framework);
    [...candidates].filter(urn => !selectedUrns.has(urn)).forEach(urn => delete entitiesCache[urn]);

    // augmentation
    const augmented = Object.assign({}, incoming, this._entitiesMetricsAugmentation(incoming));

    // update entities cache
    Object.keys(augmented).forEach(urn => entitiesCache[urn] = augmented[urn]);

    this.setProperties({ _entitiesCache: entitiesCache, _timeseriesCache: timeseriesCache, _pendingEntitiesRequests: pending });
    this.notifyPropertyChange('_timeseriesCache');
    this.notifyPropertyChange('_entitiesCache');
    this.notifyPropertyChange('_pendingEntitiesRequests');
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
      const { entities } = this.getProperties('entities');

      const metricUrns = Object.keys(entities).filter(urn => entities[urn] && entities[urn].type == 'metric');
      const baselineUrns = metricUrns.map(urn => this._makeMetricBaselineUrn(urn));

      this._startRequestMissingTimeseries(metricUrns.concat(baselineUrns));
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
      .then(checkStatus)
      .then(this._extractTimeseries)
      .then(timeseries => this._completeRequestMissingTimeseries(timeseries));

    // baselines
    const baselineOffset = context.anomalyRange[0] - context.baselineRange[0];
    const baselineAnalysisStart = context.analysisRange[0] - baselineOffset;
    const baselineAnalysisEnd = context.analysisRange[1] - baselineOffset;

    const baselineUrns = [...missing].filter(urn => urn.startsWith('frontend:baseline:metric:'));
    const baselineIdString = baselineUrns.map(urn => urn.split(":")[3]).join(',');
    const baselineUrl = `/timeseries/query?metricIds=${baselineIdString}&ranges=${baselineAnalysisStart}:${baselineAnalysisEnd}&granularity=15_MINUTES&transformations=timestamp`;

    fetch(baselineUrl)
      .then(checkStatus)
      .then(this._extractTimeseries)
      .then(timeseries => this._convertMetricToBaseline(timeseries, baselineOffset))
      .then(timeseries => this._completeRequestMissingTimeseries(timeseries));

  },

  _completeRequestMissingTimeseries(incoming) {
    console.log('_completeRequestMissingTimeseries()');
    const { _pendingTimeseriesRequests: pending, _timeseriesCache: cache } =
      this.getProperties('_pendingTimeseriesRequests', '_timeseriesCache');

    Object.keys(incoming).forEach(urn => pending.delete(urn));
    Object.keys(incoming).forEach(urn => cache[urn] = incoming[urn]);

    this.setProperties({ _pendingTimeseriesRequests: pending, _timeseriesCache: cache });
    this.notifyPropertyChange('_timeseriesCache');
    this.notifyPropertyChange('_pendingTimeseriesRequests');
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

  /**
   * Calculates the baseline start and end
   * based on the current compare mode
   * @param {Object} paramsToUpdate
   * @return {Object}
   */
  _calculateBaselineRange(paramsToUpdate) {
    const {
      analysisRangeStart = this.get('analysisRangeStart'),
      analysisRangeEnd = this.get('analysisRangeEnd'),
      compareMode = this.get('compareMode')
    } = paramsToUpdate;
    const offset = {
      WoW: 1,
      Wo2W: 2,
      Wo3W: 3,
      Wo4W: 4
    }[compareMode];

    const baselineRangeStart = moment(analysisRangeStart).subtract(offset, 'weeks').valueOf();
    const baselineRangeEnd = moment(analysisRangeEnd).subtract(offset, 'weeks').valueOf();

    return {
      baselineRangeStart,
      baselineRangeEnd
    };
  },


  /**
   * Builds the context object based on new params
   */
  _buildContext(newParams, { baselineRangeStart, baselineRangeEnd}) {
    const oldParams = getProperties(this, this.get('queryParams'));
    const params = { ...oldParams, ...newParams};
    // TODO: hook up urns with route
    // currently not sure how this is used or should be passed
    const urns = this.get('context.urns');
    const anomalyRange = [params.anomalyRangeStart, params.anomalyRangeEnd];
    const baselineRange = [baselineRangeStart, baselineRangeEnd];
    const analysisRange = [params.analysisRangeStart, params.analysisRangeEnd];
    return {
      urns,
      anomalyRange,
      baselineRange,
      analysisRange
    };
  },

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
      const baselineUrns = currentUrns.map(urn => this._makeMetricBaselineUrn(urn));

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

    /**
     * Handles the rootcause_setting change event
     * and updates query params and context
     * @param {Object} newParams new parameters to update
     */
    settingsOnChange(context) {
      console.log('settingsOnChange()');
      console.log('settingsOnChange: context', context);
      
      // const queryParams = get(this, 'queryParams');
      // const paramsToUpdate = queryParams
      // .filter(param => newParams[param])
      // .reduce((hash, param) => {
      //   hash[param] = newParams[param];
      //   return hash;
      // }, {});
      // const baselineRanges = this._calculateBaselineRange(paramsToUpdate);
      // const context = this._buildContext(paramsToUpdate, baselineRanges);

      setProperties(this, {
        context
        // ...paramsToUpdate,
        // ...baselineRanges
      });
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

