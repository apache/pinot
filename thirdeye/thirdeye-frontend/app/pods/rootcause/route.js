import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

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
      granularityOptions: ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS'],
      compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],
      filterOptions: fetch(`/data/autocomplete/filters/metric/${id}`).then(res => res.json()),
      maxTime: fetch(`/data/maxDataTime/metricId/${id}`).then(res => res.json()),
      id
    });
  },

  afterModel(model, transition) {
    const maxTime = model.maxTime ? model.maxTime + 1 : moment().valueOf();

    console.log('route: maxTime', maxTime);

    const defaultParams = {
      filters: JSON.stringify({}),
      granularity: model.granularityOptions[2],
      anomalyRangeStart:  moment(maxTime).subtract(3, 'hours').valueOf(),
      anomalyRangeEnd: moment(maxTime).valueOf(),
      analysisRangeStart: moment(maxTime).endOf('day').subtract(1, 'week').valueOf(),
      analysisRangeEnd: moment(maxTime).endOf('day').valueOf(),
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
      compareModeOptions: model.compareModeOptions
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
