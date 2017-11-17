import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';


const anomalyRange = [1509044400000, 1509422400000];
const compareMode = 'WoW';
const analysisRange = [1508785200000, 1509422400000];
const urns = new Set(['thirdeye:metric:194591', 'thirdeye:dimension:countryCode:in:provided']);
const granularity = '15_MINUTES';

const testContext = { urns, anomalyRange, compareMode, analysisRange, granularity };
const testSelectedUrns = new Set([
  'thirdeye:metric:194591', 'frontend:metric:current:194591', 'frontend:metric:baseline:194591',
  'thirdeye:metric:194592', 'frontend:metric:current:194592', 'frontend:metric:baseline:194592',
  'thirdeye:metric:194591:browserName=chrome:browserName=firefox:browserName=safari',
  'frontend:metric:current:194591:browserName=chrome:browserName=firefox:browserName=safari',
  'frontend:metric:baseline:194591:browserName=chrome:browserName=firefox:browserName=safari',
  'thirdeye:event:holiday:2712391']);

const queryParamsConfig = {
  refreshModel: false,
  replace: false
};

/**
 * Helper function that checks if a query param
 * key/value pair is valid
 * @param {*} key   - query param key
 * @param {*} value - query param value
 * @return {Boolean}
 */
const isValid = (key, value) => {
  switch(key) {
    case 'granularity':
      return ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS', '7_DAYS'].includes(value);
    case 'filters':
      return value && value.length;
    case 'compareMode':
      return ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'];
    default:
      return moment(+value).isValid();
  }
};

// TODO: move this to a utils file (DRYER)
const _filterToUrn = (filters) => {
  const urns = [];
  const filterObject = JSON.parse(filters);
  Object.keys(filterObject)
    .forEach((key) => {
      const filterUrns = filterObject[key]
        .map(dimension => `thirdeye:dimension:${key}:${dimension}:provided`);
      urns.push(...filterUrns);
    });

  return urns;
};

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  queryParams: {
    granularity: queryParamsConfig,
    filters: queryParamsConfig,
    compareMode: queryParamsConfig,
    analysisRangeStart: queryParamsConfig,
    analysisRangeEnd: queryParamsConfig,
    anomalyRangeStart: queryParamsConfig,
    anomalyRangeEnd: queryParamsConfig
  },

  model(params) {
    const { rootcauseId: id } = params;

    // TODO handle error better
    if (!id) { return; }

    return RSVP.hash({
      //granularityOptions: fetch(`/data/agg/granularity/metric/${id}`).then(res => res.json()),
      granularityOptions: ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS'],
      filterOptions: fetch(`/data/autocomplete/filters/metric/${id}`).then(res => res.json()),
      maxTime: fetch(`/data/maxDataTime/metricId/${id}`).then(res => res.json()),
      metricName: fetch(`/data/metric/${id}`).then(res => res.json()).then(res => res.name),
      compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],
      id
    });
  },

  afterModel(model, transition) {
    const defaultParams = {
      filters: JSON.stringify({}),
      granularity: model.granularityOptions[0],
      anomalyRangeStart: moment().subtract(1, 'day').valueOf(),
      anomalyRangeEnd: model.maxTime || moment().valueOf(),
      analysisRangeStart: moment().subtract(1, 'week').valueOf(),
      analysisRangeEnd: model.maxTime,
      compareMode: 'WoW'
    };
    let { queryParams } = transition;

    const validParams = Object.keys(queryParams)
      .filter((param) => {
        const value = queryParams[param];
        return value && isValid(param, value);
      })
      .reduce((hash, key) => {
        hash[key] = queryParams[key];
        return hash;
      }, {});
    Object.assign(
      model,
      { queryParams: { ...defaultParams, ...validParams }}
    );
  },

  setupController(controller, model) {
    this._super(...arguments);

    const {
      filters,
      granularity,
      analysisRangeStart,
      analysisRangeEnd,
      compareMode,
      anomalyRangeStart,
      anomalyRangeEnd
    } = model.queryParams;

    const settingsConfig = {
      granularityOptions: model.granularityOptions,
      filterOptions: model.filterOptions,
      compareModeOptions: model.compareModeOptions,
      maxTime: model.maxTime
    };

    const context = {
      urns: new Set([`thirdeye:metric:${model.id}`, ..._filterToUrn(filters)]),
      anomalyRange: [anomalyRangeStart, anomalyRangeEnd],
      analysisRange: [analysisRangeStart, analysisRangeEnd],
      granularity,
      compareMode
    };

    controller.setProperties({
      // selectedUrns: testSelectedUrns,
      selectedUrns: new Set([`thirdeye:metric:${model.id}`, `frontend:metric:current:${model.id}`, `frontend:metric:baseline:${model.id}`]),
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      settingsConfig,
      // context: testContext
      context
    });
  }
});
