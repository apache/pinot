import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { toCurrentUrn, toBaselineUrn, filterPrefix, dateFormatFull } from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const queryParamsConfig = {
  refreshModel: false,
  replace: false
};

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  authService: Ember.inject.service('session'),

  queryParams: {
    metricId: queryParamsConfig,
    sessionId: queryParamsConfig,
    anomalyId: {
      refreshModel: true,
      replace: false
    }
  },

  model(params) {
    const { metricId, anomalyId, sessionId } = params;

    let metricUrn, anomalyUrn, session, anomalyContext, anomalySessions;

    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
    }

    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
    }

    if (sessionId) {
      session = fetch(`/session/${sessionId}`).then(checkStatus).catch(() => undefined);
    }

    if (anomalyUrn) {
      anomalyContext = fetch(`/rootcause/raw?framework=anomalyContext&urns=${anomalyUrn}`).then(checkStatus).catch(() => undefined);
      anomalySessions = fetch(`/session/query?anomalyId=${anomalyId}`).then(checkStatus).catch(() => undefined);
    }

    return RSVP.hash({
      metricId,
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
    const maxTime = moment().startOf('hour').valueOf();

    const defaultParams = {
      anomalyRangeStart:  moment(maxTime).subtract(3, 'hours').valueOf(),
      anomalyRangeEnd: moment(maxTime).valueOf(),
      analysisRangeStart: moment(maxTime).endOf('day').subtract(1, 'week').valueOf() + 1,
      analysisRangeEnd: moment(maxTime).endOf('day').valueOf() + 1,
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
    let routeErrors = new Set();

    // metric-initialized context
    if (metricId && metricUrn) {
      context = {
        urns: new Set([metricUrn]),
        anomalyRange,
        analysisRange,
        granularity,
        compareMode,
        anomalyUrns: new Set()
      };

      selectedUrns = new Set([metricUrn, toCurrentUrn(metricUrn), toBaselineUrn(metricUrn)]);
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
      context
    });
  }
});
