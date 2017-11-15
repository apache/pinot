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

const ROOTCAUSE_TAB_DIMENSIONS = "dimensions";
const ROOTCAUSE_TAB_METRICS = "metrics";
const ROOTCAUSE_TAB_EVENTS = "events";

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
  
  activeTab: null, // ""
  
  init() {
    this._super(...arguments);
    this.setProperties({ activeTab: ROOTCAUSE_TAB_DIMENSIONS });
  },

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
      
      setProperties(this, {
        context
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

