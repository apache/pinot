import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { toCurrentUrn, toBaselineUrn, filterPrefix, appendFilters, toFilters } from 'thirdeye-frontend/helpers/utils';
import _ from 'lodash';

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
    sessionId: queryParamsConfig
  },

  model(params) {
    const { metricId, anomalyId, sessionId } = params;

    let metricUrn, anomalyUrn, session, anomalyContext;

    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
    }

    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
    }

    if (sessionId) {
      session = fetch(`/session/${sessionId}`).then(res => res.json());
    }

    if (anomalyUrn) {
      anomalyContext = fetch(`/rootcause/raw?framework=anomalyContext&urns=${anomalyUrn}`).then(res => res.json());
    }

    return RSVP.hash({
      metricId,
      anomalyId,
      sessionId,
      metricUrn,
      anomalyUrn,
      session,
      anomalyContext
    });
  },

  afterModel(model, transition) {
    const maxTime = moment().startOf('hour').valueOf();

    const defaultParams = {
      filters: JSON.stringify({}),
      granularity: '1_HOURS',
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
      anomalyRangeEnd
    } = model.queryParams;

    const {
      anomalyId,
      metricId,
      sessionId,
      metricUrn,
      anomalyUrn,
      session,
      anomalyContext
    } = model;

    const settingsConfig = {
      granularityOptions: ['5_MINUTES', '15_MINUTES', '1_HOURS', '3_HOURS', '1_DAYS'],
      compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W']
    };

    const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];
    const analysisRange = [analysisRangeStart, analysisRangeEnd];

    const dateFormat = 'ddd, MMM D YYYY, h:mm a';

    // default blank context
    let context = {
      urns: new Set(),
      anomalyRange,
      analysisRange,
      granularity,
      compareMode
    };

    let selectedUrns = new Set();
    let sessionName = 'New Investigation (' + moment().format(dateFormat) + ')';
    let sessionText = '';
    let sessionModified = true;

    // metric-initialized context
    if (metricId && metricUrn) {
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
    if (anomalyId && anomalyUrn && anomalyContext) {
      const contextUrns = anomalyContext.map(e => e.urn);

      const baseMetricUrns = filterPrefix(contextUrns, 'thirdeye:metric:');
      const dimensionUrns = filterPrefix(contextUrns, 'thirdeye:dimension:');

      const metricUrns = baseMetricUrns.map(urn => appendFilters(urn, toFilters(dimensionUrns)));

      const anomalyRangeUrns = filterPrefix(contextUrns, 'thirdeye:timerange:anomaly:');
      const analysisRangeUrns = filterPrefix(contextUrns, 'thirdeye:timerange:analysis:');

      // thirdeye:timerange:anomaly:{start}:{end}
      const anomalyRange = _.slice(anomalyRangeUrns[0].split(':'), 3, 5).map(i => parseInt(i, 10));

      // thirdeye:timerange:analysis:{start}:{end}
      // align to local end of day
      const [rawStart, rawEnd] = _.slice(analysisRangeUrns[0].split(':'), 3, 5).map(i => parseInt(i, 10));
      const analysisRange = [moment(rawStart).startOf('day').add(1, 'day').valueOf(), moment(rawEnd).endOf('day').valueOf()];

      context = {
        urns: new Set([...baseMetricUrns, ...dimensionUrns, anomalyUrn]),
        anomalyRange,
        analysisRange,
        granularity,
        compareMode
      };

      selectedUrns = new Set([...metricUrns, ...metricUrns.map(toCurrentUrn), ...metricUrns.map(toBaselineUrn), anomalyUrn]);
      sessionName = 'New Investigation of #' + anomalyId + ' (' + moment().format(dateFormat) + ')';
      sessionText = 'Anomaly #' + anomalyId + ' occurred due to ...';
    }

    // session-initialized context
    if (sessionId && session) {
      context = {
        urns: new Set(session.contextUrns),
        anomalyRange: [session.anomalyRangeStart, session.anomalyRangeEnd],
        analysisRange: [session.analysisRangeStart, session.analysisRangeEnd],
        granularity: session.granularity,
        compareMode: session.compareMode
      };
      selectedUrns = new Set(session.selectedUrns);
      sessionName = session.name;
      sessionText = session.text;
      sessionModified = false;
    }

    controller.setProperties({
      sessionId,
      sessionName,
      sessionText,
      sessionModified,
      settingsConfig,
      selectedUrns,
      context
    });
  }
});
