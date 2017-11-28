import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { toCurrentUrn, toBaselineUrn } from 'thirdeye-frontend/helpers/utils';

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
    case 'metricId':
      return !value || (Number.isInteger(value) && parseInt(value) >= 0);
    case 'anomalyId':
      return !value || (Number.isInteger(value) && parseInt(value) >= 0);
    case 'shareId':
      return true;
    case 'metricUrn':
      return !value || value.startsWith('thirdeye:metric:');
    case 'anomalyUrn':
      return !value || value.startsWith('thirdeye:event:anomaly:');
    case 'share':
      return !value || JSON.parse(value);
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
    metricId: queryParamsConfig,
    anomalyId: queryParamsConfig,
    shareId: queryParamsConfig
  },

  model(params) {
    const { metricId, anomalyId, shareId } = params;

    let metricUrn;
    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
    }

    let anomalyUrn;
    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
    }

    let share;
    if (shareId) {
      share = fetch(`/config/rootcause-share/${shareId}`).then(res => res.json());
    }

    console.log('route: model: metricUrn anomalyUrn, share', metricUrn, anomalyUrn, share);

    return RSVP.hash({
      metricId,
      anomalyId,
      shareId,
      metricUrn,
      anomalyUrn,
      share
    });
  },

  afterModel(model, transition) {
    const maxTime = moment().valueOf();

    const defaultParams = {
      filters: JSON.stringify({}),
      granularity: '1_HOURS',
      anomalyRangeStart:  moment(maxTime).subtract(3, 'hours').valueOf(),
      anomalyRangeEnd: moment(maxTime).valueOf(),
      analysisRangeStart: moment(maxTime).endOf('day').subtract(1, 'week').valueOf(),
      analysisRangeEnd: moment(maxTime).endOf('day').valueOf(),
      compareMode: 'WoW',
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

    return Object.assign(
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
      anomalyRangeEnd,
    } = model.queryParams;

    const {
      metricUrn,
      anomalyUrn,
      share
    } = model;

    console.log('route: setupController: metricUrn anomalyUrn, share', metricUrn, anomalyUrn, share);

    const settingsConfig = {
      granularityOptions: ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS'],
      compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W']
    };

    const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];
    const analysisRange = [analysisRangeStart, analysisRangeEnd];

    // default blank context
    let context = {
      urns: new Set(),
      anomalyRange,
      analysisRange,
      granularity,
      compareMode
    };

    let selectedUrns = new Set();

    // metric-initialized context
    if (metricUrn) {
      console.log('route: setupController: initializing context from metric mode');
      context = {
        urns: new Set([metricUrn, ..._filterToUrn(filters)]),
        anomalyRange,
        analysisRange,
        granularity,
        compareMode
      };

      selectedUrns = new Set([metricUrn, toCurrentUrn(metricUrn), toBaselineUrn(metricUrn)]);
    }

    // anomaly-initialized context
    if (anomalyUrn) {
      console.log('route: setupController: initializing context from anomaly mode');
      // TODO
    }

    // share-initialized context
    if (share) {
      console.log('route: setupController: initializing context from share mode');
      context = share.context;
      context.urns = new Set(context.urns);
      selectedUrns = new Set(share.selectedUrns);
    }

    controller.setProperties({
      settingsConfig,
      selectedUrns,
      context
    });
  }
});
