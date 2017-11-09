import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';


const anomalyRange = [1509044400000, 1509422400000];
const baselineRange = [1508439600000, 1508817600000];
const analysisRange = [1508785200000, 1509422400000];
const urns = new Set(['thirdeye:metric:194591', 'thirdeye:dimension:countryCode:in:provided']);
const granularity = '30_MINUTES';

const queryParamsConfig = {
  refreshModel: false,
  replace: false
};

const isValid = (key, value) => { 
  switch(key) {
    case 'granularity':
      return ['MINUTES', 'HOURS', 'DAYS'].includes(value);
    case 'filters':
      return value && value.length;
    default:
      return moment(+value);
  }
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
      anomalyRangeEnd: model.maxTime,
      analysisRangeStart: moment().subtract(1, 'week').valueOf(), 
      analysisRangeEnd: model.maxTime,
      compareMode: 'WoW'
    };
    let { queryParams } = transition;
    // TODO: write utils functions that checks key values in queryParams
    //       so only valid strings are authorized
    const validParams = Object.keys(queryParams)
      .filter((param) => {
        const value = queryParams[param];
        return value && isValid(param, value);
      })
      .reduce((hash, key) => {
        hash[key] = queryParams[key];
        return hash;
      }, {});
      Object.assign(model, { queryParams: { ...defaultParams, ...validParams }});
      debugger;
  },

  setupController(controller, model) {
    this._super(...arguments);
    console.log('route: setupController()');
    // TODO get initial attributes from query params
    controller.setProperties({
      selectedUrns: new Set([
        'thirdeye:metric:194591', 'frontend:baseline:metric:194591',
        'thirdeye:metric:194592', 'frontend:baseline:metric:194592',
        'thirdeye:event:holiday:2712391']),
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      context: { urns, anomalyRange, baselineRange, analysisRange, granularity },
      // context: { urns, anomalyRange, baselineRange, analysisRange, granularity },
      ...model.queryParams
    });
  }
});
