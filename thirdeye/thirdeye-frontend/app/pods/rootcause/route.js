import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import fetch from 'fetch';
import config from 'thirdeye-frontend/config/environment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import {
  toCurrentUrn,
  toBaselineUrn,
  dateFormatFull,
  appendFilters,
  filterPrefix,
  makeTime,
  value2filter
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';
import moment from 'moment';
import { get } from '@ember/object';

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
    granularity[0] = 15;
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
  const time = makeTime(parseInt(maxTime, 10));
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
    minute: -120,
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

/**
 * Returns a Set of urns augmented for virtual frontend metrics.
 * @example ([thirdeye:metric:1, thirdeye:event:holiday:2]) =>
 * [thirdeye:metric:1, frontend:metric:current:1, frontend:metric:baseline:1, thirdeye:event:holiday:2]
 */
const augmentFrontendMetrics = (urns) => {
  return [...urns]
    .filter(urn => urn.startsWith('thirdeye:metric:'))
    .map(urn => [urn, toCurrentUrn(urn), toBaselineUrn(urn)])
    .reduce((agg, urns) => agg.concat(urns), [])
    .concat([...urns].filter(urn => !urn.startsWith('thirdeye:metric:')));
};

/**
 * A variation of Object.assign() that returns the original and overrides undefined values only
 */
const assignDefaults = (base, defaults) => {
  Object.keys(base)
    .filter(key => typeof(base[key]) === 'undefined')
    .forEach(key => base[key] = defaults[key]);
  return base;
};

/**
 * Returns the array for start/end dates of the analysis range
 */
const toAnalysisRangeArray = (anomalyStart, anomalyEnd, metricGranularity) => {
  const analysisRangeStartOffset = toAnalysisOffset(metricGranularity);
  const analysisRangeEnd = makeTime(anomalyEnd).startOf('day').add(1, 'day').valueOf();
  const analysisRangeStart = makeTime(anomalyStart).startOf('day').add(analysisRangeStartOffset, 'day').valueOf();
  return [analysisRangeStart, analysisRangeEnd];
};

export default Route.extend(AuthenticatedRouteMixin, {
  authService: service('session'),
  session: service(),

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

  /**
   * Helper for the RCA's default header title
   * @param {String} metric's name
   * @param {Array} timeRange is either the anomalyRange anomaly time range or analysisRange display time range
   * @param {Object} entity is either anomalyEntity or metricEntity
   * @return {Object} trimmed { anomalyRange, analysisRange }
   */
  _makeTitleHeader: function(metricName, timeRange, entity) {
    const timeStart = timeRange[0] ? moment(timeRange[0]).format('MM/DD/YYYY') : moment(entity.start).format('MM/DD/YYYY');
    const timeEnd = timeRange[1] ? moment(timeRange[1]).format('MM/DD/YYYY') : moment(entity.end).format('MM/DD/YYYY');
    const timeDisplay = timeStart === timeEnd ? timeStart : `${timeStart} to ${timeEnd}`;//Show one for daily
    return `Investigation on ${metricName} for ${timeDisplay}`;
  },

  model(params) {
    const { metricId, sessionId, anomalyId } = params;
    let { contextUrnsInit, selectedUrnsInit, anomalyUrnsInit, anomalyRangeInit, analysisRangeInit, compareModeInit, granularityInit } = params;
    const isDevEnv = config.environment === 'development';

    let metricUrn, metricEntity, session, anomalyUrn, anomalyTemplateEntity, anomalySessions, metricTemplate;

    if (metricId) {
      metricUrn = `thirdeye:metric:${metricId}`;
      metricEntity = fetch(`/rootcause/raw?framework=identity&urns=${metricUrn}`).then(checkStatus).then(res => res[0]).catch(() => {});
      metricTemplate = fetch(`/rootcause/template/search?metricId=${metricId}`).then(checkStatus).then(res => (res) ? res[0] : null).catch(() => {});
    }

    if (anomalyId) {
      anomalyUrn = `thirdeye:event:anomaly:${anomalyId}`;
      anomalyTemplateEntity = fetch(`/rootcause/raw?framework=identity&urns=${anomalyUrn}`)
        .then(checkStatus)
        .then(res => {
          const anomalyMetricId = res[0].attributes.metricId[0];
          const entityFromResponse = res[0];
          // retrieve metric template for the corresponding metric
          return fetch(`/rootcause/template/search?metricId=${anomalyMetricId}`)
            .then(checkStatus)
            .then(res1 => (res1) ? res1[0] : null)
            .then(res => {
              metricTemplate = res;
              return {
                template: res,
                entity: entityFromResponse
              };
            }).catch(() => {});
        }).catch(() => {});
      anomalySessions = fetch(`/session/query?anomalyId=${anomalyId}`).then(checkStatus).catch(() => {});
    }

    if (sessionId) {
      session = fetch(`/session/${sessionId}`).then(checkStatus).catch(() => {});
    }

    let anomalyRange;
    if (anomalyRangeInit) {
      anomalyRange = anomalyRangeInit.split(',').map(r => parseInt(r, 10));
    }

    let analysisRange;
    if (analysisRangeInit) {
      analysisRange = analysisRangeInit.split(',').map(r => parseInt(r, 10));
    }

    let contextUrnsPredefined;
    if (contextUrnsInit) {
      contextUrnsPredefined = new Set(contextUrnsInit.split(','));
    }

    let selectedUrnsPredefined;
    if (selectedUrnsInit) {
      selectedUrnsPredefined = new Set(augmentFrontendMetrics(selectedUrnsInit.split(',')));
    }

    let anomalyUrnsPredefined;
    if (anomalyUrnsInit) {
      anomalyUrnsPredefined = new Set(anomalyUrnsInit.split(','));
    }

    return RSVP.hash({
      isDevEnv,
      metricId,
      metricUrn,
      metricEntity,
      sessionId,
      session,
      anomalyId,
      anomalyUrn,
      anomalyTemplateEntity,
      anomalySessions,
      contextUrnsPredefined,
      selectedUrnsPredefined,
      anomalyUrnsPredefined,
      anomalyRange,
      analysisRange,
      metricTemplate,
      granularity: granularityInit,
      compareMode: compareModeInit
    });
  },

  /**
   * @description Resets any query params to allow not to have leak state or sticky query-param
   * @method resetController
   * @param {Object} controller - active controller
   * @param {Boolean} isExiting - exit status
   * @return {undefined}
   */
  resetController(controller, isExiting) {
    this._super(...arguments);
    if (isExiting) {
      controller.set('sessionId', null);
    }
  },

  afterModel(model, transition) {
    const defaultParams = {
      anomalyRange: [
        makeTime().startOf('hour').subtract(3, 'hour').valueOf(),
        makeTime().startOf('hour').valueOf()
      ],
      analysisRange: [
        makeTime().startOf('day').subtract(6, 'day').valueOf(),
        makeTime().startOf('day').add(1, 'day').valueOf()
      ],
      granularity: '1_HOURS',
      compareMode: 'wo1w'
    };

    // default params
    assignDefaults(model, defaultParams);

    const {anomalySessions} = model;
    // load latest saved session for anomaly
    if (!_.isEmpty(anomalySessions)) {

      const mostRecent = _.last(_.sortBy(anomalySessions, 'updated'));
      model.anomalyId = null;
      model.anomalyUrn = null;
      model.anomalyContext = null;
      model.sessionId = mostRecent.id;
      model.session = mostRecent;

      // NOTE: apparently this does not abort the ongoing transition
      this.transitionTo({ queryParams: { sessionId: mostRecent.id, anomalyId: null } });
    }

    return model;
  },

  setupController(controller, model) {
    this._super(...arguments);

    const {
      analysisRange,
      anomalyRange,
      granularity,
      compareMode,
      metricId,
      metricUrn,
      metricEntity,
      sessionId,
      session,
      anomalyId,
      anomalyUrn,
      anomalyTemplateEntity,
      contextUrnsPredefined,
      selectedUrnsPredefined,
      anomalyUrnsPredefined
    } = model;

    /* 
      To ensure that the call to search for metric template happens after getting metric id of an anomaly, 
      we put anomalyEntity and metricTemplate under anomalyTemplateEntity
    */
    const metricTemplate = (model.metricTemplate) ? model.metricTemplate : (anomalyTemplateEntity || {}).template;
    const anomalyEntity = (anomalyTemplateEntity || {}).entity;

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
    let sessionName = 'New Investigation (' + makeTime().format(dateFormatFull) + ')';
    let sessionText = '';
    let sessionOwner = this.get('authService.data.authenticated.name');
    let sessionPermissions = 'READ_WRITE';
    let sessionUpdatedBy = '';
    let sessionUpdatedTime = '';
    let sessionModified = true;
    let sessionTableSettings = null;
    let sessionUserCustomized = false;
    let setupMode = ROOTCAUSE_SETUP_MODE_CONTEXT;
    let routeErrors = new Set();
    let baseMetricUrn = null;

    // metric-initialized context
    if (metricId && metricUrn) {
      if (!_.isEmpty(metricEntity)) {
        const granularity = adjustGranularity(metricEntity.attributes.granularity[0]);
        const metricGranularity = toMetricGranularity(granularity);
        const maxTime = adjustMaxTime(metricEntity.attributes.maxTime[0], metricGranularity);

        const anomalyRangeEnd = makeTime(maxTime).startOf(metricGranularity[1]).valueOf();
        const anomalyRangeStartOffset = toAnomalyOffset(metricGranularity);
        const anomalyRangeStart = makeTime(anomalyRangeEnd).add(anomalyRangeStartOffset, metricGranularity[1]).valueOf();
        const anomalyRange = [anomalyRangeStart, anomalyRangeEnd];

        // align to local end of day
        const analysisRange = toAnalysisRangeArray(anomalyRangeEnd, anomalyRangeEnd, metricGranularity);

        context = {
          urns: new Set([metricUrn]),
          anomalyRange,
          analysisRange,
          granularity: (granularity === '1_DAYS') ? '1_HOURS' : granularity,
          compareMode,
          anomalyUrns: new Set()
        };

        selectedUrns = new Set([metricUrn, toCurrentUrn(metricUrn), toBaselineUrn(metricUrn)]);
        const metricName = metricEntity.label.split('::')[1];
        sessionName = this._makeTitleHeader(metricName, anomalyRange, metricEntity);
        setupMode = ROOTCAUSE_SETUP_MODE_SELECTED;
      }
    }



    // anomaly-initialized context
    if (anomalyId && anomalyUrn) {
      if (!_.isEmpty(anomalyEntity)) {
        const granularity = adjustGranularity(anomalyEntity.attributes.metricGranularity[0]);
        const metricGranularity = toMetricGranularity(granularity);
        const anomalyRange = [parseInt(anomalyEntity.start, 10), parseInt(anomalyEntity.end, 10)];
        // align to local end of day (anomalyStart, anomalyEnd, metricGranularity)
        const analysisRange = toAnalysisRangeArray(anomalyRange[0], anomalyRange[1], metricGranularity);

        const anomalyDimNames = anomalyEntity.attributes['dimensions'] || [];
        const anomalyFilters = [];
        anomalyDimNames.forEach(dimName => {
          anomalyEntity.attributes[dimName].forEach(dimValue => {
            anomalyFilters.pushObject(value2filter(dimName, dimValue));
          });
        });

        const anomalyMetricUrnRaw = `thirdeye:metric:${anomalyEntity.attributes['metricId'][0]}`;
        const anomalyMetricUrn = appendFilters(anomalyMetricUrnRaw, anomalyFilters);

        const anomalyFunctionUrns = [];
        const anomalyFunctionUrnRaw = `thirdeye:event:anomaly:${anomalyId}`;
        anomalyFunctionUrns.pushObject(appendFilters(anomalyFunctionUrnRaw, anomalyFilters));

        context = {
          urns: new Set([anomalyMetricUrn]),
          anomalyRange,
          analysisRange,
          granularity,
          compareMode: 'WoW',
          anomalyUrns: new Set([anomalyUrn, anomalyMetricUrn].concat(anomalyFunctionUrns))
        };
        selectedUrns = new Set([anomalyUrn, anomalyMetricUrn]);
        const metricName = anomalyEntity.attributes.metric[0];
        sessionName = this._makeTitleHeader(metricName, anomalyRange, anomalyEntity);
        setupMode = ROOTCAUSE_SETUP_MODE_SELECTED;
        sessionText = anomalyEntity.attributes.comment[0];
      } else {
        routeErrors.add(`Could not find anomalyId ${anomalyId}`);
      }
    }
    // rca template context  
    if(metricTemplate) {
      const dimAnalysisModule = metricTemplate.modules[0];
      sessionTableSettings = {
        oneSideError: dimAnalysisModule.configuration.oneSideError,
        orderType: (dimAnalysisModule.configuration.manualOrder == true) ? 'manual' : 'auto',
        summarySize: dimAnalysisModule.configuration.summarySize,
        depth: dimAnalysisModule.configuration.dimensionDepth,
        dimensions: dimAnalysisModule.configuration.includedDimension,
        excludedDimensions: (dimAnalysisModule.configuration.excludedDimension) ? dimAnalysisModule.configuration.excludedDimension : []
      };
      sessionUserCustomized = true;
    }
    // session-initialized context
    if (sessionId) {
      if (!_.isEmpty(session)) {
        const { name, text, updatedBy, updated, owner, permissions,
          isUserCustomizingRequest, customTableSettings } = model.session;
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
        sessionTableSettings = customTableSettings;
        sessionUserCustomized = isUserCustomizingRequest;

      } else {
        routeErrors.add(`Could not find sessionId ${sessionId}`);
      }
    }

    // overrides
    if (!_.isEmpty(contextUrnsPredefined)) {
      context.urns = contextUrnsPredefined;
      setupMode = ROOTCAUSE_SETUP_MODE_NONE;
    }

    if (!_.isEmpty(selectedUrnsPredefined)) {
      selectedUrns = selectedUrnsPredefined;
      setupMode = ROOTCAUSE_SETUP_MODE_NONE;
    }

    if (!_.isEmpty(anomalyUrnsPredefined)) {
      context.anomalyUrns = anomalyUrnsPredefined;
      setupMode = ROOTCAUSE_SETUP_MODE_NONE;
    }

    // update secondary metrics
    const sizeMetricUrns = new Set(filterPrefix(context.urns, 'thirdeye:metric:'));

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
      sessionTableSettings,
      sessionUserCustomized,
      selectedUrns,
      sizeMetricUrns,
      setupMode,
      context,

      // reset overrides
      contextUrnsInit: undefined,
      selectedUrnsInit: undefined,
      anomalyUrnsInit: undefined,
      anomalyRangeInit: undefined,
      analysisRangeInit: undefined,
      granularityInit: undefined,
      compareModeInit: undefined
    });
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    },

    error() {
      return true;
    },

    /**
    * Refresh route's model.
    * @method refreshModel
    * @return {undefined}
    */
    refreshModel() {
      this.refresh();
    }
  }
});
