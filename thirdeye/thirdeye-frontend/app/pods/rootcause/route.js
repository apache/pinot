import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import {
  toCurrentUrn,
  toBaselineUrn,
  filterPrefix,
  dateFormatFull
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_SETUP_MODE_CONTEXT = "context";
const ROOTCAUSE_SETUP_MODE_SELECTED = "selected";
const ROOTCAUSE_SETUP_MODE_NONE = "none";

/**
 * converts RCA backend granularity strings into units understood by moment.js
 */
const toMetricGranularity = (attrGranularity) => {
  const UNIT_MAPPING = {
    MINUTES: 'minute',
    HOURS: 'hour',
    DAYS: 'day'
  };

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
    hour: -3,
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
    const { metricId, anomalyId, sessionId } = params;

    let metricUrn, metricEntity, anomalyUrn, session, anomalyContext, anomalySessions;

    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
      metricEntity = fetch(`/rootcause/raw?framework=identity&urns=${metricUrn}`).then(checkStatus).then(res => res[0]).catch(() => {});
    }

    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
    }

    if (sessionId) {
      session = fetch(`/session/${sessionId}`).then(checkStatus).catch(() => {});
    }

    if (anomalyUrn) {
      anomalyContext = fetch(`/rootcause/raw?framework=anomalyContext&urns=${anomalyUrn}`).then(checkStatus).catch(() => {});
      anomalySessions = fetch(`/session/query?anomalyId=${anomalyId}`).then(checkStatus).catch(() => {});
    }

    return RSVP.hash({
      metricId,
      metricEntity,
      anomalyId,
      sessionId,
      metricUrn,
      anomalyUrn,
      session,
      anomalyContext,
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
      anomalyId,
      metricId,
      metricEntity,
      sessionId,
      metricUrn,
      anomalyUrn,
      session,
      anomalyContext
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
        const maxTime = parseInt(metricEntity.attributes.maxTime[0], 10);
        const granularity = metricEntity.attributes.granularity[0];
        const metricGranularity = toMetricGranularity(granularity);

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
      if (!_.isEmpty(anomalyContext)) {
        const contextUrns = anomalyContext.map(e => e.urn);

        const metricUrns = filterPrefix(contextUrns, 'thirdeye:metric:');

        const anomalyRangeUrns = filterPrefix(contextUrns, 'thirdeye:timerange:anomaly:');
        const analysisRangeUrns = filterPrefix(contextUrns, 'thirdeye:timerange:analysis:');

        // thirdeye:timerange:anomaly:{start}:{end}
        const anomalyRange = _.slice(anomalyRangeUrns[0].split(':'), 3, 5).map(i => parseInt(i, 10));

        // thirdeye:timerange:analysis:{start}:{end}
        // align to local end of day
        const [rawStart, rawEnd] = _.slice(analysisRangeUrns[0].split(':'), 3, 5).map(i => parseInt(i, 10));
        const analysisRange = [moment(rawStart).startOf('day').add(1, 'day').valueOf(), moment(rawEnd).endOf('day').valueOf()];

        context = {
          urns: new Set([...metricUrns]),
          anomalyRange,
          analysisRange,
          granularity,
          compareMode,
          anomalyUrns: new Set([...metricUrns, anomalyUrn])
        };

        selectedUrns = new Set([...metricUrns, ...metricUrns.map(toCurrentUrn), ...metricUrns.map(toBaselineUrn), anomalyUrn]);
        sessionName = 'New Investigation of #' + anomalyId + ' (' + moment().format(dateFormatFull) + ')';
        setupMode = ROOTCAUSE_SETUP_MODE_SELECTED;

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
