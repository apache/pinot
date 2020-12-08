import { observer, computed, get, set, getProperties, setProperties } from '@ember/object';
import { later, debounce } from '@ember/runloop';
import { reads, gt, or } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import Controller from '@ember/controller';
import {
  filterObject,
  filterPrefix,
  toBaselineUrn,
  toCurrentUrn,
  toOffsetUrn,
  dateFormatFull,
  appendTail,
  stripTail,
  extractTail,
  makeTime
} from 'thirdeye-frontend/utils/rca-utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/shared/eventTableColumns';
import filterBarConfig from 'thirdeye-frontend/shared/filterBarConfig';
import fetch from 'fetch';
import config from 'thirdeye-frontend/config/environment';
import _ from 'lodash';

const ROOTCAUSE_TAB_DIMENSIONS = 'dimensions';
const ROOTCAUSE_TAB_METRICS = 'metrics';
const ROOTCAUSE_TAB_TREND = 'trend';

const ROOTCAUSE_SETUP_MODE_CONTEXT = 'context';
const ROOTCAUSE_SETUP_MODE_SELECTED = 'selected';
const ROOTCAUSE_SETUP_MODE_NONE = 'none';

const ROOTCAUSE_SETUP_EVENTS_SCORE_THRESHOLD = Number.POSITIVE_INFINITY;
const ROOTCAUSE_SETUP_METRICS_SCORE_THRESHOLD = Number.POSITIVE_INFINITY;

const ROOTCAUSE_SERVICE_ROUTE = 'route';
const ROOTCAUSE_SERVICE_ENTITIES = 'entities';
const ROOTCAUSE_SERVICE_TIMESERIES = 'timeseries';
const ROOTCAUSE_SERVICE_AGGREGATES = 'aggregates';
const ROOTCAUSE_SERVICE_BREAKDOWNS = 'breakdowns';
const ROOTCAUSE_SERVICE_ANOMALY_FUNCTIONS = 'anomalyFunctions';
const ROOTCAUSE_SERVICE_CALLGRAPH = 'callgraph';
const ROOTCAUSE_SERVICE_ALL = 'all';

const ROOTCAUSE_SESSION_TIMER_INTERVAL = 300000;
const ROOTCAUSE_SESSION_DEBOUNCE_INTERVAL = 2000;

const ROOTCAUSE_SESSION_PERMISSIONS_READ = 'READ';
const ROOTCAUSE_SESSION_PERMISSIONS_READ_WRITE = 'READ_WRITE';

// TODO: Update module import to comply by new Ember standards

export default Controller.extend({
  queryParams: [
    'metricId',
    'sessionId',
    'anomalyId',
    'contextUrnsInit',
    'selectedUrnsInit',
    'anomalyUrnsInit',
    'anomalyRangeInit',
    'analysisRangeInit',
    'granularityInit',
    'compareModeInit'
  ],

  //
  // notifications
  //

  /**
   * Errors from routing
   * @type {Set}
   */
  routeErrors: null, // Set

  /**
   * Warning for concurrent session modification
   * @type {string}
   */
  sessionUpdateWarning: null,

  //
  // services
  //
  authService: service('session'),

  entitiesService: service('services/rootcause-entities-cache'),

  timeseriesService: service('services/rootcause-timeseries-cache'),

  aggregatesService: service('services/rootcause-aggregates-cache'),

  breakdownsService: service('services/rootcause-breakdowns-cache'),

  scoresService: service('services/rootcause-scores-cache'),

  sessionService: service('services/rootcause-session-datasource'),

  anomalyFunctionService: service('services/rootcause-anomalyfunction-cache'),

  callgraphService: service('services/rootcause-callgraph-cache'),

  sessionTemplateService: service('services/rootcause-template'),

  //
  // user details
  //
  username: reads('authService.data.authenticated.name'),

  //
  // user selection
  //

  /**
   * rootcause search context
   * @type {object}
   *
   * {
   *   urns: Set,
   *   anomalyUrns: Set,
   *   anomalyRange: [2],
   *   analysisRange: [2],
   *   compareMode: string
   *   granularity: string
   * }
   */
  context: null,

  /**
   * entity urns selected for display
   * @type {Set}
   */
  selectedUrns: null,

  /**
   * entity urns marked as invisible
   * @type {Set}
   */
  invisibleUrns: null,

  /**
   * entity urns currently being hovered over
   * @type {Set}
   */
  hoverUrns: null,

  /**
   * (event) entity urns passing the filter side-bar
   * @type {Set}
   */
  filteredUrns: null,

  secondaryUrns: null,

  /**
   * displayed investigation tab ('metrics', 'dimensions', ...)
   * @type {string}
   */
  activeTab: ROOTCAUSE_TAB_DIMENSIONS,

  /**
   * displayed investigation sub-tabs
   * @type {Object}
   */
  activeSubTabs: null,

  /**
   * display mode for timeseries chart
   * @type {string}
   */
  timeseriesMode: 'split',

  /**
   * urns of the currently focused entities in the legend component
   * @type {string}
   */
  focusedUrns: null,

  /**
   * toggle for running _setupForMetric() on selection of the first metric
   * @type {boolean}
   */
  setupMode: ROOTCAUSE_SETUP_MODE_NONE,

  /**
   * toggle for displaying verbose error messages
   * @type {boolean}
   */
  verbose: false,

  //
  // session data
  //

  /**
   * rootcause session title (on top)
   * @type {string}
   */
  sessionName: null,

  /**
   * rootcause session comments (on top)
   * @type {string}
   */
  sessionText: null,

  /**
   * rootcause session modification indicator
   * @type {boolean}
   */
  sessionModified: null,

  /**
   * rootcause session update timestamp
   * @type {int}
   */
  sessionUpdated: null,

  /**
   * rootcause session last edit author
   * @type {string}
   */
  sessionUpdatedBy: null,

  /**
   * rootcause session owner
   * @type {string}
   */
  sessionOwner: null,

  /**
   * anomaly feedback type. To be overridden by manual update
   * @type {string}
   */
  anomalyFeedback: computed('anomalyUrn', 'entities', function () {
    const { anomalyUrn, entities } = getProperties(this, 'anomalyUrn', 'entities');

    try {
      return entities[anomalyUrn].attributes.status[0];
    } catch (ignore) {
      return 'NO_FEEDBACK';
    }
  }),

  //
  // static component config
  //

  /**
   * side-bar filter config
   * @type {object}
   */
  filterConfig: filterBarConfig,

  /**
   * flag that toogles the modal view
   * @type {Boolean}
   */
  showEntityMappingModal: false,

  /**
   * Default settings
   */
  init() {
    this._super(...arguments);
    setProperties(this, {
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      secondaryUrns: new Set(),
      activeSubTabs: {
        dimensions: 'algorithm'
      }
    });
    // This is a flag for the acceptance test for rootcause to prevent it from timing out because of this run loop
    if (config.environment !== 'test') {
      later(this, this._onCheckSessionTimer, ROOTCAUSE_SESSION_TIMER_INTERVAL);
    }
  },

  /**
   * Context observer manages subscriptions to data feeds.
   *
   * Manages entities, timeseries, aggregates, and breakdowns. Pushes context modifications to data
   * services to refresh caches on-demand. Changes propagate throughout the application via the respective
   * computed properties ('entities', 'timeseries', 'aggregates', 'breakdowns')
   *
   * entities:         rootcause search results, such as events and metrics
   *                   (typically displayed in event table, timeseries chart)
   *
   * timeseries:       time-ordered metric values for display in chart
   *                   (typically displayed in timeseries chart)
   *
   * aggregates:       metrics values summarized over multiple time windows (anomaly, baseline, ...)
   *                   (typically displayed in metrics table, anomaly header)
   *
   * breakdowns:       de-aggregated metric values over multiple time windows (anomaly, baseline, ...)
   *                   (typically displayed in dimension heatmap)
   *
   * scores:           entity scores as computed by backend pipelines (e.g. metric anomality score)
   *                   (typically displayed in metrics table)
   *
   * anomalyfunctions: anomaly function baselines for display in chart
   *                   (typically displayed in timeseries chart)
   *
   * callgraph:        service call graph edges as ranked by the backend
   *                   (typically displayed in call graph table)
   */
  _contextObserver: observer('context', 'entities', 'selectedUrns', 'sizeMetricUrns', 'activeTab', function () {
    const {
      context,
      selectedUrns,
      sizeMetricUrns,
      entitiesService,
      timeseriesService,
      aggregatesService,
      breakdownsService,
      scoresService,
      anomalyFunctionService,
      callgraphService,
      activeTab,
      setupMode
    } = getProperties(
      this,
      'context',
      'selectedUrns',
      'sizeMetricUrns',
      'entitiesService',
      'timeseriesService',
      'aggregatesService',
      'breakdownsService',
      'scoresService',
      'anomalyFunctionService',
      'callgraphService',
      'activeTab',
      'setupMode'
    );
    if (!context || !selectedUrns) {
      return;
    }

    if (setupMode === ROOTCAUSE_SETUP_MODE_CONTEXT) {
      return;
    }

    //
    // entities
    //
    const entitiesUrns = new Set([...selectedUrns, ...context.urns, ...context.anomalyUrns]);
    entitiesService.request(context, entitiesUrns);

    //
    // related metrics
    //
    const anomalyMetricUrns = new Set();
    const relatedMetricUrns = new Set();

    if (activeTab === ROOTCAUSE_TAB_METRICS || activeTab === ROOTCAUSE_TAB_TREND) {
      const entities = get(this, 'entitiesService.entities'); // cache may be stale, fetch directly
      filterPrefix(Object.keys(entities), 'thirdeye:metric:').forEach((urn) => relatedMetricUrns.add(urn));
    }

    if (context.anomalyUrns.size > 0) {
      filterPrefix(context.anomalyUrns, 'thirdeye:metric:').forEach((urn) => anomalyMetricUrns.add(urn));
    }

    //
    // timeseries
    //
    const timeseriesUrns = new Set(selectedUrns);

    if (activeTab === ROOTCAUSE_TAB_TREND) {
      [...relatedMetricUrns].forEach((urn) => {
        timeseriesUrns.add(toCurrentUrn(urn));
        timeseriesUrns.add(toBaselineUrn(urn));
      });
    }

    timeseriesService.request(context, timeseriesUrns);

    //
    // anomaly function baselines
    //
    const anomalyFunctionUrns = filterPrefix(context.anomalyUrns, 'thirdeye:event:anomaly');
    anomalyFunctionService.request(context, new Set(anomalyFunctionUrns));

    //
    // breakdowns
    //
    if (activeTab === ROOTCAUSE_TAB_DIMENSIONS) {
      const metricUrns = new Set(filterPrefix(context.urns, 'thirdeye:metric:'));
      const currentUrns = [...metricUrns].map(toCurrentUrn);
      const baselineUrns = [...metricUrns].map(toBaselineUrn);
      const sizeMetricCurrentUrns = [...sizeMetricUrns].map(toCurrentUrn);
      breakdownsService.request(context, new Set(currentUrns.concat(baselineUrns).concat(sizeMetricCurrentUrns)));
    }

    //
    // scores
    //
    if (activeTab === ROOTCAUSE_TAB_METRICS) {
      const scoresUrns = new Set(relatedMetricUrns);
      scoresService.request(context, new Set(scoresUrns));
    }

    //
    // aggregates
    //
    const offsets =
      context.compareMode === 'forecast'
        ? ['current', 'baseline', 'yo1y', 'upper', 'lower']
        : ['current', 'baseline', 'wo1w', 'wo2w'];
    const offsetUrns = [...relatedMetricUrns]
      .map((urn) => [].concat(offsets.map((offset) => toOffsetUrn(urn, offset))))
      .reduce((agg, l) => agg.concat(l), []);

    const anomalyOffsets = ['current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];
    const anomalyOffsetUrns = [...anomalyMetricUrns]
      .map((urn) => [].concat(anomalyOffsets.map((offset) => toOffsetUrn(urn, offset))))
      .reduce((agg, l) => agg.concat(l), []);

    //
    // call graph
    //
    if (activeTab === ROOTCAUSE_SERVICE_CALLGRAPH) {
      callgraphService.request(context, [...context.urns]);
    }

    aggregatesService.request(context, new Set([...offsetUrns, ...anomalyOffsetUrns]));
  }),

  /**
   * Setup observer for context and default selection
   * May run multiple times while entities are loading.
   */
  _setupObserver: observer('context', 'entities', 'scores', 'setupMode', function () {
    const { setupMode } = getProperties(this, 'setupMode');

    switch (setupMode) {
      case ROOTCAUSE_SETUP_MODE_NONE:
        // left blank
        break;

      case ROOTCAUSE_SETUP_MODE_CONTEXT:
        this._setupContext();
        break;

      case ROOTCAUSE_SETUP_MODE_SELECTED:
        this._setupSelected();
        break;

      default:
        throw new Error(`Unknown setup mode '${setupMode}'`);
    }
  }),

  //
  // Public properties (computed)
  //

  /**
   * Subscribed entities cache
   * @type {object}
   */
  entities: reads('entitiesService.entities'),

  /**
   * Subscribed timeseries cache (metrics, anomaly baselines)
   * @type {object}
   */
  timeseries: computed('timeseriesService.timeseries', 'anomalyFunctionService.timeseries', 'context', function () {
    const { timeseriesService, anomalyFunctionService, context } = getProperties(
      this,
      'timeseriesService',
      'anomalyFunctionService',
      'context'
    );

    const timeseries = Object.assign({}, timeseriesService.timeseries);

    if (context.compareMode !== 'predicted') {
      return timeseries;
    }

    // NOTE: only supports a single anomaly function baseline
    const anomalyFunctionUrns = filterPrefix(context.anomalyUrns, 'thirdeye:event:anomaly:');
    const anomalyFunctionUrn = anomalyFunctionUrns[0];

    filterPrefix(context.anomalyUrns, 'thirdeye:metric:').forEach((urn) => {
      timeseries[toBaselineUrn(urn)] = anomalyFunctionService.timeseries[anomalyFunctionUrn];
    });

    return timeseries;
  }),

  /**
   * Subscribed aggregates cache
   * @type {object}
   */
  aggregates: reads('aggregatesService.aggregates'),

  /**
   * Subscribed breakdowns cache
   * @type {object}
   */
  breakdowns: reads('breakdownsService.breakdowns'),

  /**
   * Subscribed scores cache
   * @type {object}
   */
  scores: reads('scoresService.scores'),

  /**
   * Subscribed callgraph edges cache
   * @type {object}
   */
  edges: reads('callgraphService.edges'),

  /**
   * Primary metric urn for rootcause search
   * @type {string}
   */
  metricUrn: computed('context', function () {
    const { context } = getProperties(this, 'context');
    const metricUrns = filterPrefix(context.urns, 'thirdeye:metric:');
    if (!metricUrns) {
      return false;
    }
    return metricUrns[0];
  }),

  /**
   * Number of data point remaining in service queue for metrics table
   * @type {int}
   */
  metricsPendingCount: computed('aggregatesService.pending', 'scoresService.pending', function () {
    const nAggregates = get(this, 'aggregatesService.pending').size;
    const nScores = get(this, 'scoresService.pending').size;

    return nAggregates + nScores;
  }),

  /**
   * Primary metric urn for rootcause search
   * @type {string}
   */
  sizeMetricUrn: computed('sizeMetricUrns', function () {
    const { sizeMetricUrns } = getProperties(this, 'sizeMetricUrns');
    const metricUrns = filterPrefix(sizeMetricUrns, 'thirdeye:metric:');
    if (!metricUrns) {
      return false;
    }
    return metricUrns[0];
  }),

  /**
   * Primary anomaly urn for anomaly header
   * @type {string}
   */
  anomalyUrn: computed('anomalyUrns', function () {
    const { context } = getProperties(this, 'context');

    const anomalyEventUrn = filterPrefix(context.anomalyUrns, 'thirdeye:event:anomaly:');

    if (!anomalyEventUrn) {
      return;
    }

    return anomalyEventUrn[0];
  }),

  /**
   * Visible series and events in timeseries chart
   * @type {array}
   */
  chartSelectedUrns: computed('entities', 'selectedUrns', 'invisibleUrns', function () {
    const { selectedUrns, invisibleUrns } = getProperties(this, 'selectedUrns', 'invisibleUrns');

    const urns = new Set(selectedUrns);
    [...invisibleUrns].forEach((urn) => urns.delete(urn));

    return urns;
  }),

  /**
   * (Event) entities for event table as filtered by the side bar
   * @type {object}
   */
  eventTableEntities: computed('entities', 'filteredUrns', function () {
    const { entities, filteredUrns } = getProperties(this, 'entities', 'filteredUrns');
    return filterObject(entities, (e) => filteredUrns.has(e.urn));
  }),

  /**
   * Columns config for event table
   * @type {object}
   */
  eventTableColumns: EVENT_TABLE_COLUMNS,

  /**
   * (Event) entities for filtering in the side bar
   * @type {object}
   */
  eventFilterEntities: computed('entities', function () {
    const { entities } = getProperties(this, 'entities');
    return filterObject(entities, (e) => e.type == 'event');
  }),

  //
  // loading indicators
  //
  isLoadingEntities: gt('entitiesService.pending.size', 0),

  isLoadingTimeseries: gt('timeseriesService.pending.size', 0),

  isLoadingAggregates: gt('aggregatesService.pending.size', 0),

  isLoadingBreakdowns: gt('breakdownsService.pending.size', 0),

  isLoadingScores: gt('scoresService.pending.size', 0),

  isLoadingAnomalyFunctions: gt('anomalyFunctionService.pending.size', 0),

  isLoadingMetricData: or('isLoadingAggregates', 'isLoadingScores'),

  isLoadingCallgraph: gt('callgraphService.pending.size', 0),

  loadingFrameworks: reads('entitiesService.pending'),

  //
  // error indicators
  //
  hasErrorsRoute: gt('routeErrors.size', 0),

  hasErrorsEntities: gt('entitiesService.errors.size', 0),

  hasErrorsTimeseries: gt('timeseriesService.errors.size', 0),

  hasErrorsAggregates: gt('aggregatesService.errors.size', 0),

  hasErrorsBreakdowns: gt('breakdownsService.errors.size', 0),

  hasErrorsScores: gt('scoresService.errors.size', 0),

  hasErrorsAnomalyFunctions: gt('scoresService.errors.size', 0),

  hasServiceErrors: or(
    'hasErrorsEntities',
    'hasErrorsTimeseries',
    'hasErrorsAggregates',
    'hasErrorsBreakdowns',
    'hasErrorsScores',
    'hasErrorsAnomalyFunctions'
  ),

  //
  // session handling
  //
  sessionCanSave: computed('sessionPermissions', 'sessionOwner', 'username', function () {
    const { sessionOwner, sessionPermissions, username } = getProperties(
      this,
      'sessionOwner',
      'sessionPermissions',
      'username'
    );

    if (sessionPermissions === ROOTCAUSE_SESSION_PERMISSIONS_READ_WRITE) {
      return true;
    }
    return sessionOwner === username;
  }),

  sessionCanCopy: computed('sessionId', 'sessionPermissions', 'sessionOwner', 'username', function () {
    const { sessionId, sessionOwner, sessionPermissions, username } = getProperties(
      this,
      'sessionId',
      'sessionOwner',
      'sessionPermissions',
      'username'
    );

    // NOTE: these conditions are temporary until full design for session copy is available

    if (_.isEmpty(sessionId)) {
      return false;
    }

    if (sessionOwner === username) {
      return false;
    } // temporary

    if (sessionPermissions === ROOTCAUSE_SESSION_PERMISSIONS_READ) {
      return true;
    }

    return false; // temporary
  }), // Ember.computed.bool('sessionId') - when enabled

  /**
   * Sets the transient rca session properties after saving
   *
   * @param sessionId rca session id
   * @private
   */
  _updateSession(sessionId) {
    const { username, metricId, anomalyId } = getProperties(this, 'username', 'metricId', 'anomalyId');

    setProperties(this, {
      sessionId,
      sessionUpdatedBy: username,
      sessionUpdatedTime: makeTime().valueOf(),
      sessionModified: false
    });

    const queryParams = {};
    queryParams['sessionId'] = sessionId;

    if (!_.isEmpty(metricId)) {
      queryParams['metricId'] = null;
    }

    if (!_.isEmpty(anomalyId)) {
      queryParams['anomalyId'] = null;
    }

    this.transitionToRoute({ queryParams });
  },

  /**
   * Serializes the current controller state for persistence as rca session
   *
   * @returns serialized rca session state
   * @private
   */
  _makeSession() {
    const {
      context,
      selectedUrns,
      sessionId,
      sessionName,
      sessionText,
      sessionOwner,
      sessionPermissions,
      isUserCustomizingRequest,
      customTableSettings
    } = getProperties(
      this,
      'context',
      'selectedUrns',
      'sessionId',
      'sessionName',
      'sessionText',
      'sessionOwner',
      'sessionPermissions',
      'isUserCustomizingRequest',
      'customTableSettings'
    );

    return {
      id: sessionId,
      name: sessionName,
      text: sessionText,
      owner: sessionOwner,
      permissions: sessionPermissions,
      compareMode: context.compareMode,
      granularity: context.granularity,
      anomalyRangeStart: context.anomalyRange[0],
      anomalyRangeEnd: context.anomalyRange[1],
      analysisRangeStart: context.analysisRange[0],
      analysisRangeEnd: context.analysisRange[1],
      contextUrns: context.urns,
      anomalyUrns: context.anomalyUrns,
      selectedUrns,
      isUserCustomizingRequest,
      customTableSettings
    };
  },

  /**
   * Fetches the current session state from the backend and issues a notification if it has been updated
   *
   * @private
   */
  _checkSession() {
    const { sessionId, sessionUpdatedTime, sessionService } = getProperties(
      this,
      'sessionId',
      'sessionUpdatedTime',
      'sessionService'
    );

    if (!sessionId) {
      return;
    }

    sessionService
      .loadAsync(sessionId)
      .then((res) => {
        if (res.updated > sessionUpdatedTime) {
          setProperties(this, {
            sessionUpdateWarning: `This investigation (${sessionId}) was updated by ${res.updatedBy} on ${makeTime(
              res.updated
            ).format(dateFormatFull)}. Please refresh the page.`
          });
        }
      })
      .catch(() => undefined);
  },

  /**
   * Timer function checking the current session state
   *
   * @private
   */
  _onCheckSessionTimer() {
    const sessionId = get(this, 'sessionId');

    if (!sessionId) {
      return;
    }

    // debounce: do not run if destroyed
    if (this.isDestroyed) {
      return;
    }

    debounce(this, this._onCheckSessionTimer, ROOTCAUSE_SESSION_TIMER_INTERVAL);

    if (!sessionId) {
      return;
    }

    this._checkSession();
  },

  /**
   * Updates anomaly feedback without session
   *
   * @private
   */
  _updateAnomalyFeedback() {
    debounce(this, this._updateAnomalyFeedbackDebounce, ROOTCAUSE_SESSION_DEBOUNCE_INTERVAL);
  },

  /**
   * Debounced implementation for updating anomaly feedback without session
   *
   * @private
   */
  _updateAnomalyFeedbackDebounce() {
    const { anomalyUrn, anomalyFeedback, sessionText } = getProperties(
      this,
      'anomalyUrn',
      'anomalyFeedback',
      'sessionText'
    );

    if (!anomalyUrn) {
      return;
    }

    // debounce: do not run if destroyed
    if (this.isDestroyed) {
      return;
    }

    const id = anomalyUrn.split(':')[3];
    const jsonString = JSON.stringify({ feedbackType: anomalyFeedback, comment: sessionText });
    fetch(`/dashboard/anomaly-merged-result/feedback/${id}`, { method: 'POST', body: jsonString });
  },

  //
  // default selection
  //

  /**
   * Transition to route for metric id
   *
   * @private
   */
  _setupContext() {
    const { context } = getProperties(this, 'context');

    const contextMetricUrns = filterPrefix(context.urns, 'thirdeye:metric:');

    if (_.isEmpty(contextMetricUrns)) {
      return;
    }

    const metricUrn = contextMetricUrns[0];
    const metricId = metricUrn.split(':')[2];

    this.transitionToRoute({ queryParams: { metricId } });
  },

  /**
   * Select top-scoring entities by default if metric selected for the first time.
   * Idempotent addition of urns to support multiple execution while data loading.
   *
   * @private
   */
  _setupSelected() {
    const { context, entities, scores, selectedUrns, loadingFrameworks, isLoadingScores } = getProperties(
      this,
      'context',
      'entities',
      'scores',
      'selectedUrns',
      'loadingFrameworks',
      'isLoadingScores'
    );

    const newSelectedUrns = new Set(selectedUrns);

    filterPrefix(context.urns, 'thirdeye:metric:').forEach((urn) => {
      newSelectedUrns.add(urn);
      newSelectedUrns.add(toCurrentUrn(urn));
      newSelectedUrns.add(toBaselineUrn(urn));
    });

    // events
    const groupedEvents = Object.values(entities)
      .filter((e) => e.type === 'event')
      .reduce((agg, e) => {
        const type = e.eventType;
        agg[type] = agg[type] || [];
        agg[type].push(e);
        return agg;
      }, {});

    // add events passing threshold
    Object.values(groupedEvents).forEach((arr) => {
      arr.filter((e) => e.score >= ROOTCAUSE_SETUP_EVENTS_SCORE_THRESHOLD).forEach((e) => newSelectedUrns.add(e.urn));
    });

    // metrics
    filterPrefix(Object.keys(entities), 'thirdeye:metric:')
      .filter((urn) => urn in scores && scores[urn] >= ROOTCAUSE_SETUP_METRICS_SCORE_THRESHOLD)
      .forEach((urn) => {
        newSelectedUrns.add(urn);
        newSelectedUrns.add(toCurrentUrn(urn));
        newSelectedUrns.add(toBaselineUrn(urn));
      });

    if (_.isEqual(selectedUrns, newSelectedUrns)) {
      if (loadingFrameworks.size <= 0) {
        set(this, 'setupMode', ROOTCAUSE_SETUP_MODE_NONE);
      }
      return;
    }

    setProperties(this, {
      selectedUrns: newSelectedUrns,
      setupMode:
        loadingFrameworks.size > 0 || isLoadingScores ? ROOTCAUSE_SETUP_MODE_SELECTED : ROOTCAUSE_SETUP_MODE_NONE
    });
  },

  //
  // Actions
  //

  actions: {
    /**
     * Updates selected urns.
     *
     * @param {object} updates dictionary with urns to add and remove (true adds, false removes, omitted keys are left as is)
     * @returns {undefined}
     */
    onSelection(updates) {
      const { selectedUrns } = getProperties(this, 'selectedUrns');
      Object.keys(updates)
        .filter((urn) => updates[urn])
        .forEach((urn) => selectedUrns.add(urn));
      Object.keys(updates)
        .filter((urn) => !updates[urn])
        .forEach((urn) => selectedUrns.delete(urn));
      setProperties(this, {
        selectedUrns: new Set(selectedUrns),
        sessionModified: true
      });
    },

    /**
     * Closure action passed into the legend component to handle the hover interactivity.
     * Rewrites urns to highlight appropriate chart elements.
     *
     * @param {Array} urns
     */
    onLegendHover(urns) {
      const focusUrns = new Set(urns);

      filterPrefix(focusUrns, 'thirdeye:metric:').forEach((urn) => {
        focusUrns.add(toCurrentUrn(urn));
        focusUrns.add(toBaselineUrn(urn));
      });

      set(this, 'focusedUrns', focusUrns);
    },

    /**
     * Updates visible urns.
     *
     * @param {object} updates dictionary with urns to show and hide (true shows, false hides, omitted keys are left as is)
     * @returns {undefined}
     */
    onVisibility(updates) {
      const { invisibleUrns } = getProperties(this, 'invisibleUrns');
      Object.keys(updates)
        .filter((urn) => updates[urn])
        .forEach((urn) => invisibleUrns.delete(urn));
      Object.keys(updates)
        .filter((urn) => !updates[urn])
        .forEach((urn) => invisibleUrns.add(urn));

      setProperties(this, { invisibleUrns: new Set(invisibleUrns) });
    },

    /**
     * Sets the rootcause search context
     *
     * @param {Object} context new context
     * @returns {undefined}
     */
    onContext(context) {
      setProperties(this, { context, sessionModified: true });
    },

    /**
     * Sets the urns to be displayed in the (event) entity table
     *
     * @param {Iterable} urns filtered urns to be displayed
     * @returns {undefined}
     */
    onFilter(urns) {
      setProperties(this, { filteredUrns: new Set(urns) });
    },

    /**
     * Sets the display mode for timeseries
     *
     * @param {String} timeseriesMode
     * @returns {undefined}
     */
    onChart(timeseriesMode) {
      setProperties(this, { timeseriesMode });
    },

    /**
     * Sets the hover selection for the chart tooltip
     *
     * @param {Iterable} urns urns hovered over
     * @param {Int} timestamp hover timestamp
     * @returns {undefined}
     */
    chartOnHover(urns, timestamp) {
      setProperties(this, {
        hoverUrns: new Set(urns),
        hoverTimestamp: timestamp
      });
    },

    /**
     * Sets the session name and text
     *
     * @param {String} name session name/title
     * @param {String} text session summary
     * @returns {undefined}
     */
    onSessionChange(name, text) {
      setProperties(this, {
        sessionName: name,
        sessionText: text,
        sessionModified: true
      });
      this._updateAnomalyFeedback();
    },

    /**
     * Saves the session to the backend. Overrides existing session, if any.
     *
     * @returns {undefined}
     */
    onSessionSave() {
      const { sessionService, sessionCanSave, sessionTemplateService, context } = getProperties(
        this,
        'sessionService',
        'sessionCanSave',
        'sessionTemplateService',
        'context'
      );

      if (sessionCanSave) {
        const session = this._makeSession();
        const contextMetricUrns = filterPrefix(context.urns, 'thirdeye:metric:');
        const metricUrn = contextMetricUrns[0];
        sessionTemplateService.saveDimensionAnalysisAsync(
          metricUrn,
          session.customTableSettings.dimensions,
          session.customTableSettings.excludedDimensions,
          session.customTableSettings.orderType == 'manual',
          session.customTableSettings.oneSideError,
          session.customTableSettings.summarySize,
          session.customTableSettings.depth
        );
        return sessionService
          .saveAsync(session)
          .then((sessionId) => this._updateSession(sessionId))
          .catch(() => {
            const { routeErrors } = getProperties(this, 'routeErrors');
            routeErrors.add('Could not save investigation');
            setProperties(this, { routeErrors: new Set(routeErrors) });
          });
      }
    },

    /**
     * Saves a dedicated copy the session to the backend and updates the session id.
     *
     * @returns {undefined}
     */
    onSessionCopy() {
      const { sessionId, sessionName, sessionService, sessionCanCopy } = getProperties(
        this,
        'sessionId',
        'sessionName',
        'sessionService',
        'sessionCanCopy'
      );

      if (sessionCanCopy) {
        set(this, 'sessionName', `Copy of ${sessionName}`);

        const session = this._makeSession();

        // copy, reference old session
        delete session['id'];
        session['previousId'] = sessionId;

        return sessionService
          .saveAsync(session)
          .then((sessionId) => this._updateSession(sessionId))
          .catch(() => {
            const { routeErrors } = getProperties(this, 'routeErrors');
            routeErrors.add('Could not copy investigation');
            setProperties(this, { routeErrors: new Set(routeErrors) });
          });
      }
    },

    /**
     * Saves the anomaly feedback to the backend. Overrides existing feedback, if any.
     *
     * @param {String} anomalyUrn anomaly entity urn
     * @param {String} feedback anomaly feedback type string
     */
    onFeedback(anomalyUrn, feedback) {
      set(this, 'anomalyFeedback', feedback);
      this._updateAnomalyFeedback();
    },

    /**
     * Selects a new primary urn for the search context.
     *
     * @param {object} updates (see onSelection, extracts "thirdeye:metric:" only)
     * @returns {undefined}
     */
    onPrimaryChange(updates) {
      const { context, sizeMetricUrns } = getProperties(this, 'context', 'sizeMetricUrns');

      // NOTE: updates here do not conform to standard. Only newly selected urns are passed in, removed are omitted.

      const addedUrns = Object.keys(updates).filter((urn) => updates[urn]);
      const removedUrns = [...context.urns].filter((urn) => !addedUrns.includes(urn));

      // primary metrics
      const urns = new Set(context.urns);
      addedUrns.forEach((urn) => urns.add(urn));
      removedUrns.forEach((urn) => urns.delete(urn));

      // secondary metrics
      const newSizeMetricUrns = new Set();
      const addedBaseUrns = filterPrefix(addedUrns, 'thirdeye:metric:').map((urn) => stripTail(urn));
      const removedBaseUrns = filterPrefix(removedUrns, 'thirdeye:metric:').map((urn) => stripTail(urn));

      const metricUrns = filterPrefix(addedUrns, 'thirdeye:metric:');

      if (_.isEqual(addedBaseUrns, removedBaseUrns)) {
        // only filter changed
        const tails = metricUrns.map((urn) => extractTail(urn));
        tails.forEach((tail) => {
          sizeMetricUrns.forEach((urn) => newSizeMetricUrns.add(appendTail(stripTail(urn), tail)));
        });
      } else {
        metricUrns.forEach((urn) => newSizeMetricUrns.add(urn));
      }

      set(this, 'sizeMetricUrns', newSizeMetricUrns);

      const newContext = Object.assign({}, context, { urns });
      this.send('onContext', newContext);
    },

    /**
     * Updates selected urns by adding the current primary metric.
     *
     * @returns {undefined}
     */
    onPrimarySelection() {
      const { context } = getProperties(this, 'context');

      const metricUrns = filterPrefix(context.urns, 'thirdeye:metric:');
      const currentUrns = metricUrns.map(toCurrentUrn);
      const baselineUrns = metricUrns.map(toBaselineUrn);

      const updates = [...metricUrns, ...currentUrns, ...baselineUrns].reduce((agg, urn) => {
        agg[urn] = true;
        return agg;
      }, {});

      this.send('onSelection', updates);
    },

    /**
     * Selects a new anomalyRange and compareMode for the search context.
     *
     * @param {Int} start anomaly range start (in millis)
     * @param {Int} end anomaly range end (in millis)
     * @param {String} compareMode
     */
    onComparisonChange(start, end, compareMode) {
      const { context } = getProperties(this, 'context');

      // adjust display window if necessary
      let analysisRange = [...context.analysisRange];
      if (analysisRange[0] >= start) {
        analysisRange[0] = makeTime(start).startOf('day').valueOf();
      }
      if (analysisRange[1] <= end) {
        //not sure we need this now? -lohuynh
        analysisRange[1] = makeTime(end).startOf('day').add(1, 'days').valueOf();
      }

      const newContext = Object.assign({}, context, {
        anomalyRange: [start, end],
        analysisRange,
        compareMode
      });

      this.send('onContext', newContext);
    },

    heatmapOnSizeMetric(sizeMetricUrn) {
      // TODO make this multi metric with "updates"
      set(this, 'sizeMetricUrns', new Set([sizeMetricUrn]));
    },

    /**
     * Clears error logs of data services and/or route
     */
    clearErrors(type) {
      const {
        entitiesService,
        timeseriesService,
        aggregatesService,
        breakdownsService,
        anomalyFunctionService,
        callgraphService
      } = getProperties(
        this,
        'entitiesService',
        'timeseriesService',
        'aggregatesService',
        'breakdownsService',
        'anomalyFunctionService',
        'callgraphService'
      );

      switch (type) {
        case ROOTCAUSE_SERVICE_ENTITIES:
          entitiesService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_TIMESERIES:
          timeseriesService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_AGGREGATES:
          aggregatesService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_BREAKDOWNS:
          breakdownsService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_ANOMALY_FUNCTIONS:
          anomalyFunctionService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_CALLGRAPH:
          callgraphService.clearErrors();
          break;

        case ROOTCAUSE_SERVICE_ROUTE:
          setProperties(this, { routeErrors: new Set() });
          break;

        case ROOTCAUSE_SERVICE_ALL:
          entitiesService.clearErrors();
          timeseriesService.clearErrors();
          aggregatesService.clearErrors();
          breakdownsService.clearErrors();
          anomalyFunctionService.clearErrors();
          callgraphService.clearErrors();
          break;
      }
    },

    /**
     * Toggles the modal view
     */
    onEntityMappingClick() {
      set(this, 'showEntityMappingModal', true);
    },

    /**
     * Handles the modal submit action
     * Flushes the cache to reload the related entities
     */
    onModalSubmit() {
      get(this, 'entitiesService').flushCache();
      this.notifyPropertyChange('context');
      set(this, 'showEntityMappingModal', false);
    },

    /**
     * Toggles the create event modal view
     */
    onCreateEventClick() {
      set(this, 'showCreateEventModal', true);
    },

    /**
     * Save dimensions-algorithm table settings to post to session (called by child)
     */
    saveTableSettings(customTableSettings, isUserCustomizingRequest) {
      this.setProperties({
        customTableSettings,
        isUserCustomizingRequest
      });
    }
  }
});
