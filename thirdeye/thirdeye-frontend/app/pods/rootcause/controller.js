import Ember from 'ember';
import { makeIterable, filterObject, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import config from 'thirdeye-frontend/mocks/filterBarConfig';
import moment from 'moment';

const {
  getProperties,
  get,
  setProperties
} = Ember;

export default Ember.Controller.extend({
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

  //
  // Public properties (computed)
  //

  options: Ember.computed('model', function() {
    const model = this.get('model');
    return getProperties(model, ['granularityOptions', 'filterOptions', 'maxTime', 'compareModeOptions']);
  }),

  selectedOptions: Ember.computed(function() {
    const queryParams = get(this, 'queryParams');

    return queryParams.reduce((hash, param) => {
      const value = this.queryToOption(param, get(this, param));
      
      hash[param] = value;
      return hash;
    }, {});
  }),

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

    settingsOnChange(newParams) {
      console.log('settingsOnChange()');
      console.log('settingsOnChange: context', newParams);
      
      const queryParams = get(this, 'queryParams');
      
      const paramsToUpdate = queryParams
      .filter(param => newParams[param])
      .reduce((hash, param) => {
        hash[param] = newParams[param]; 
        return hash;
      }, {});
      
      const analysisRangeStart = paramsToUpdate.analysisRangeStart || this.get('analysisRangeStart');
      const analysisRangeEnd = paramsToUpdate.analysisRangeEnd || this.get('analysisRangeEnd');
      const compareMode = paramsToUpdate.compareMode || this.get('compareMode');

      //set baselineStart and End

      const offset = {
        WoW: 1,
        Wo2W: 2,
        Wo3W: 3,
        Wo4W: 4
      }[compareMode];

      const baselineRangeStart = moment(analysisRangeStart).subtract(offset, 'weeks').valueOf();
      const baselineRangeEnd = moment(analysisRangeEnd).subtract(offset, 'weeks').valueOf();

      setProperties(this, {
        ...paramsToUpdate,
        baselineRangeStart,
        baselineRangeEnd
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

