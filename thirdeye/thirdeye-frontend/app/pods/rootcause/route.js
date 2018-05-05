import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import {
  toCurrentUrn,
  toBaselineUrn,
  dateFormatFull,
  appendFilters
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_SETUP_MODE_CONTEXT = "context";
const ROOTCAUSE_SETUP_MODE_SELECTED = "selected";
const ROOTCAUSE_SETUP_MODE_NONE = "none";

const UNIT_MAPPING = {
  NANOSECONDS: 'nanosecond',
  MILLISECONDS: 'millisecond',
  SECONDS: 'second',
  MINUTES: 'minute',
  HOURS: 'hour',
  DAYS: 'day'
};

/**
 * adjusts RCA backend granularity to a sane scale
 */
const adjustGranularity = (attrGranularity) => {
  const [count, unit] = attrGranularity.split('_');
  const granularity = [parseInt(count, 10), unit];

  if (['NANOSECONDS', 'MILLISECONDS', 'SECONDS'].includes(granularity[1])) {
    granularity[0] = 5;
    granularity[1] = 'MINUTES';
  }

  if (['MINUTES'].includes(granularity[1])) {
    granularity[0] = Math.max(granularity[0], 5);
    granularity[1] = 'MINUTES';
  }

  return granularity[0] + "_" + granularity[1];
};

/**
 * adjusts metric max time based on metric granularity
 */
const adjustMaxTime = (maxTime, metricGranularity) => {
  const time = moment(parseInt(maxTime, 10));
  const [count, unit] = metricGranularity;

  const start = time.startOf(unit);
  const remainder = start.get(unit) % count;

  return start.add(-1 * remainder, unit);
};

/**
 * converts RCA backend granularity strings into units understood by moment.js
 */
const toMetricGranularity = (attrGranularity) => {
  const [count, unit] = attrGranularity.split('_');
  return [parseInt(count, 10), UNIT_MAPPING[unit]];
};

/**
 * Returns the anomaly time range offset (in granularity units) based on metric granularity
 */
const toAnomalyOffset = (granularity) => {
  const UNIT_MAPPING = {
    minute: -30,
    hour: -3,
    day: -1
  };
  return UNIT_MAPPING[granularity[1]] || -1;
};

/**
 * Returns the analysis time range offset (in days) based on metric granularity
 */
const toAnalysisOffset = (granularity) => {
  const UNIT_MAPPING = {
    minute: -1,
    hour: -2,
    day: -7
  };
  return UNIT_MAPPING[granularity[1]] || -1;
};

export default Route.extend(AuthenticatedRouteMixin, {
  authService: service('session'),

  queryParams: {
    metricId: {
      refreshModel: true,
      replace: true
    },
    sessionId: {
      refreshModel: false,
      replace: false
    },
    anomalyId: {
      refreshModel: true,
      replace: false
    }
  },

  model(params) {
    const { metricId, sessionId, anomalyId } = params;

    let metricUrn, metricEntity, session, anomalyUrn, anomalyEntity, anomalySessions;

    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
      metricEntity = fetch(`/rootcause/raw?framework=identity&urns=${metricUrn}`).then(checkStatus).then(res => res[0]).catch(() => {});
    }

    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
      anomalyEntity = fetch(`/rootcause/raw?framework=identity&urns=${anomalyUrn}`).then(checkStatus).then(res => res[0]).catch(() => {});
      anomalySessions = fetch(`/session/query?anomalyId=${anomalyId}`).then(checkStatus).catch(() => {});
    }

    if (sessionId) {
      session = fetch(`/session/${sessionId}`).then(checkStatus).catch(() => {});
    }

    return RSVP.hash({
      metricId,
      metricUrn,
      metricEntity,
      sessionId,
      session,
      anomalyId,
      anomalyUrn,
      anomalyEntity,
      anomalySessions
    });
  },

  afterModel(model, transition) {
    const defaultParams = {
      anomalyRangeStart: moment().startOf('hour').subtract(3, 'hour').valueOf(),
      anomalyRangeEnd: moment().startOf('hour').valueOf(),
      analysisRangeStart: moment().startOf('day').subtract(6, 'day').valueOf(),
      analysisRangeEnd: moment().startOf('day').add(1, 'day').valueOf(),
      granularity: '1_HOURS',
      compareMode: 'WoW'
    };

    // default params
    const { queryParams } = transition;
    const newModel = Object.assign(model, { ...defaultParams, ...queryParams });

    // load latest saved session for anomaly
    const { anomalySessions } = model;
    if (!_.isEmpty(anomalySessions)) {
      const mostRecent = _.last(_.sortBy(anomalySessions, 'updated'));

      Object.assign(newModel, {
        anomalyId: null,
        anomalyUrn: null,
        anomalyContext: null,
        sessionId: mostRecent.id,
        session: mostRecent
      });

      // NOTE: apparently this does not abort the ongoing transition
      this.transitionTo({ queryParams: { sessionId: mostRecent.id, anomalyId: null } });
    }

    return newModel;
  },

  setupController(controller, model) {
    this._super(...arguments);

    const {
      analysisRangeStart,
      analysisRangeEnd,
      granularity,
      compareMode,
      anomalyRangeStart,
      anomalyRangeEnd,
      metricId,
      metricUrn,
      metricEntity,
      sessionId,
      session,
      anomalyId,
      anomalyUrn,
      anomalyEntity
    } = model;

    const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];
    const analysisRange = [analysisRangeStart, analysisRangeEnd];

    // default blank context
    let context = {
      urns: new Set(),
      anomalyRange,
      analysisRange,
      granularity,
      compareMode,
      anomalyUrns: new Set()
    };

    let selectedUrns = new Set();
    let sessionName = 'New Investigation (' + moment().format(dateFormatFull) + ')';
    let sessionText = '';
    let sessionOwner = this.get('authService.data.authenticated.name');
    let sessionPermissions = 'READ_WRITE';
    let sessionUpdatedBy = '';
    let sessionUpdatedTime = '';
    let sessionModified = true;
    let setupMode = ROOTCAUSE_SETUP_MODE_CONTEXT;
    let routeErrors = new Set();

    // metric-initialized context
    if (metricId && metricUrn) {
      if (!_.isEmpty(metricEntity)) {
        const granularity = adjustGranularity(metricEntity.attributes.granularity[0]);
        const metricGranularity = toMetricGranularity(granularity);
        const maxTime = adjustMaxTime(metricEntity.attributes.maxTime[0], metricGranularity);

        const anomalyRangeEnd = moment(maxTime).startOf(metricGranularity[1]).valueOf();
        const anomalyRangeStartOffset = toAnomalyOffset(metricGranularity);
        const anomalyRangeStart = moment(anomalyRangeEnd).add(anomalyRangeStartOffset, metricGranularity[1]).valueOf();
        const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];

        const analysisRangeEnd = moment(anomalyRangeEnd).startOf('day').add(1, 'day').valueOf();
        const analysisRangeStartOffset = toAnalysisOffset(metricGranularity);
        const analysisRangeStart = moment(anomalyRangeEnd).add(analysisRangeStartOffset, 'day').valueOf();
        const analysisRange = [analysisRangeStart, analysisRangeEnd];

        context = {
          urns: new Set([metricUrn]),
          anomalyRange,
          analysisRange,
          granularity: (granularity === '1_DAYS') ? '1_HOURS' : granularity,
          compareMode,
          anomalyUrns: new Set()
        };

        selectedUrns = new Set([metricUrn, toCurrentUrn(metricUrn), toBaselineUrn(metricUrn)]);
        setupMode = ROOTCAUSE_SETUP_MODE_SELECTED;
      }
    }

    // anomaly-initialized context
    if (anomalyId && anomalyUrn) {
      if (!_.isEmpty(anomalyEntity)) {
        const granularity = adjustGranularity(anomalyEntity.attributes.metricGranularity[0]);
        const metricGranularity = toMetricGranularity(granularity);

        const anomalyRange = [parseInt(anomalyEntity.start, 10), parseInt(anomalyEntity.end, 10)];

        // align to local end of day
        const analysisRangeEnd = moment(anomalyRange[1]).startOf('day').add(1, 'day').valueOf();
        const analysisRangeStartOffset = toAnalysisOffset(metricGranularity);
        const analysisRangeStart = moment(anomalyRange[0]).startOf('day').add(analysisRangeStartOffset, 'day').valueOf();
        const analysisRange = [analysisRangeStart, analysisRangeEnd];

        const anomalyDimNames = anomalyEntity.attributes['dimensions'] || [];
        const anomalyFilters = anomalyDimNames.map(dimName => [dimName, anomalyEntity.attributes[dimName]]);

        const anomalyMetricUrnRaw = `thirdeye:metric:${anomalyEntity.attributes['metricId'][0]}`;
        const anomalyMetricUrn = appendFilters(anomalyMetricUrnRaw, anomalyFilters);

        const anomalyFunctionUrnRaw = `frontend:anomalyfunction:${anomalyEntity.attributes['functionId'][0]}`;
        const anomalyFunctionUrn = appendFilters(anomalyFunctionUrnRaw, anomalyFilters);

        context = {
          urns: new Set([anomalyMetricUrn]),
          anomalyRange,
          analysisRange,
          granularity,
          compareMode: 'WoW',
          anomalyUrns: new Set([anomalyUrn, anomalyMetricUrn, anomalyFunctionUrn])
        };

        selectedUrns = new Set([anomalyUrn, anomalyMetricUrn]);
        sessionName = 'New Investigation of #' + anomalyId + ' (' + moment().format(dateFormatFull) + ')';
        setupMode = ROOTCAUSE_SETUP_MODE_SELECTED;
        sessionText = anomalyEntity.attributes.comment[0];
      } else {
        routeErrors.add(`Could not find anomalyId ${anomalyId}`);
      }
    }

    // session-initialized context
    if (sessionId) {
      if (!_.isEmpty(session)) {
        const { name, text, updatedBy, updated, owner, permissions } = model.session;
        context = {
          urns: new Set(session.contextUrns),
          anomalyRange: [session.anomalyRangeStart, session.anomalyRangeEnd],
          analysisRange: [session.analysisRangeStart, session.analysisRangeEnd],
          granularity: session.granularity,
          compareMode: session.compareMode,
          anomalyUrns: new Set(session.anomalyUrns || [])
        };
        selectedUrns = new Set(session.selectedUrns);

        sessionName = name;
        sessionText = text;
        sessionOwner = owner;
        sessionPermissions = permissions;
        sessionUpdatedBy = updatedBy;
        sessionUpdatedTime = updated;
        sessionModified = false;
        setupMode = ROOTCAUSE_SETUP_MODE_NONE;

      } else {
        routeErrors.add(`Could not find sessionId ${sessionId}`);
      }
    }

    controller.setProperties({
      routeErrors,
      anomalyId,
      metricId,
      sessionId,
      sessionName,
      sessionText,
      sessionOwner,
      sessionPermissions,
      sessionUpdatedBy,
      sessionUpdatedTime,
      sessionModified,
      selectedUrns,
      setupMode,
      context
    });
  }
});
