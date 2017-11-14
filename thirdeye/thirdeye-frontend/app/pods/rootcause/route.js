import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';


// const anomalyRange = [1509044400000, 1509422400000];
// const baselineRange = [1508439600000, 1508817600000];
// const analysisRange = [1508785200000, 1509422400000];
// const urns = new Set(['thirdeye:metric:194591', 'thirdeye:dimension:countryCode:in:provided']);
// const granularity = '30_MINUTES';

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
      return ['MINUTES', 'HOURS', 'DAYS'].includes(value);
    case 'filters':
      return value && value.length;
    case 'compareMode':
      return ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'];
    default:
      return moment(+value).isValid();
  }
};

// TODO: move into utils
const _calculateBaselineRange = (range, compareMode) => {
  const [
    start,
    end
  ] = range;
  const offset = {
    WoW: 1,
    Wo2W: 2,
    Wo3W: 3,
    Wo4W: 4
  }[compareMode];

  const baselineRangeStart = moment(start).subtract(offset, 'weeks').valueOf();
  const baselineRangeEnd = moment(end).subtract(offset, 'weeks').valueOf();

  return [ baselineRangeStart, baselineRangeEnd ];
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
      granularityOptions: fetch(`/data/agg/granularity/metric/${id}`).then(res => res.json()),
      filterOptions: fetch(`/data/autocomplete/filters/metric/${id}`).then(res => res.json()),
      maxTime: fetch(`/data/maxDataTime/metricId/${id}`).then(res => res.json()),
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
    console.log('route: setupController()');

    const {
      filters,
      granularity,
      analysisRangeStart,
      analysisRangeEnd,
      compareMode,
      anomalyRangeStart,
      anomalyRangeEnd
    } = model.queryParams;

    const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];
    const analysisRange = [analysisRangeStart, analysisRangeEnd];
    const baselineRange = _calculateBaselineRange(anomalyRange, compareMode);
    const urns = new Set([`thirdeye:metric:${model.id}`, ..._filterToUrn(filters)]);

    const context = {
      urns,
      anomalyRange,
      baselineRange,
      analysisRange,
      granularity,
      compareMode
    };

    controller.setProperties({
      selectedUrns: new Set([
        'thirdeye:metric:194591', 'frontend:baseline:metric:194591',
        'thirdeye:metric:194592', 'frontend:baseline:metric:194592',
        'thirdeye:event:holiday:2712391']),
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      context,
      ...model.queryParams
    });
  }
});
