import Ember from 'ember';
import { filterObject, filterPrefix, toBaselineUrn, toCurrentUrn, toOffsetUrn, toFilters, appendFilters, dateFormatFull } from 'thirdeye-frontend/utils/rca-utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import filterBarConfig from 'thirdeye-frontend/mocks/filterBarConfig';
import fetch from 'fetch';
import moment from 'moment';
import config from 'thirdeye-frontend/config/environment';
import _ from 'lodash';

const ROOTCAUSE_TAB_DIMENSIONS = 'dimensions';
const ROOTCAUSE_TAB_METRICS = 'metrics';

const ROOTCAUSE_SERVICE_ROUTE = 'route';
const ROOTCAUSE_SERVICE_ENTITIES = 'entities';
const ROOTCAUSE_SERVICE_TIMESERIES = 'timeseries';
const ROOTCAUSE_SERVICE_AGGREGATES = 'aggregates';
const ROOTCAUSE_SERVICE_BREAKDOWNS = 'breakdowns';

const ROOTCAUSE_SESSION_TIMER_INTERVAL = 300000;

const ROOTCAUSE_SESSION_PERMISSIONS_READ = 'READ';
const ROOTCAUSE_SESSION_PERMISSIONS_READ_WRITE = 'READ_WRITE';

// TODO: Update module import to comply by new Ember standards

export default Ember.Controller.extend({
  queryParams: [
    'metricId',
    'anomalyId',
    'sessionId'
  ],

  //
  // notifications
  //
  routeErrors: null, // Set

  sessionUpdateWarning: null, // string

  //
  // services
  //
  authService: Ember.inject.service('session'),

  entitiesService: Ember.inject.service('rootcause-entities-cache'),

  timeseriesService: Ember.inject.service('rootcause-timeseries-cache'),

  aggregatesService: Ember.inject.service('rootcause-aggregates-cache'),

  breakdownsService: Ember.inject.service('rootcause-breakdowns-cache'),

  scoresService: Ember.inject.service('rootcause-scores-cache'),

  sessionService: Ember.inject.service('rootcause-session-datasource'),

  //
  // user details
  //
  username: Ember.computed.reads('authService.data.authenticated.name'),

  //
  // user selection
  //

  /**
   * rootcause search context
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
  context: null, //

  /**
   * entity urns selected for display
   */
  selectedUrns: null, // Set

  /**
   * entity urns marked as invisible
   */
  invisibleUrns: null, // Set

  /**
   * entity urns currently being hovered over
   */
  hoverUrns: null, // Set

  /**
   * (event) entity urns passing the filter side-bar
   */
  filteredUrns: null,

  /**
   * displayed investigation tab ('metrics', 'dimensions', ...)
   */
  activeTab: null, // ""

  /**
   * display mode for timeseries chart
   */
  timeseriesMode: null, // ""

  /**
   * urn of the currently focused entity in the legend component
   */
  focusedUrn: null,

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

  //
  // static component config
  //

  /**
   * side-bar filter config
   */
  filterConfig: filterBarConfig, // {}

  /**
   * Default settings
   */
  init() {
    this._super(...arguments);
    this.setProperties({
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      activeTab: ROOTCAUSE_TAB_METRICS,
      timeseriesMode: 'split'
    });

    // This is a flag for the acceptance test for rootcause to prevent it from timing out because of this run loop
    if (config.environment !== 'test') {
      Ember.run.later(this, this._onCheckSessionTimer, ROOTCAUSE_SESSION_TIMER_INTERVAL);
    }
  },

  /**
   * Context observer manages subscriptions to data feeds.
   *
   * Manages entities, timeseries, aggregates, and breakdowns. Pushes context modifications to data
   * services to refresh caches on-demand. Changes propagate throughout the application via the respective
   * computed properties ('entities', 'timeseries', 'aggregates', 'breakdowns')
   *
   * entities:     rootcause search results, such as events and metrics
   *               (typically displayed in event table, timeseries chart)
   *
   * timeseries:   time-ordered metric values for display in chart
   *               (typically displayed in timeseries chart)
   *
   * aggregates:   metrics values summarized over multiple time windows (anomaly, baseline, ...)
   *               (typically displayed in metrics table, anomaly header)
   *
   * breakdowns:   de-aggregated metric values over multiple time windows (anomaly, baseline, ...)
   *               (typically displayed in dimension heatmap)
   * 
   * scores:       entity scores as computed by backend pipelines (e.g. metric anomality score)
   *               (typically displayed in metrics table)
   */
  _contextObserver: Ember.observer(
    'context',
    'entities',
    'selectedUrns',
    'entitiesService',
    'timeseriesService',
    'aggregatesService',
    'breakdownsService',
    'activeTab',
    function () {
      const { context, selectedUrns, entitiesService, timeseriesService, aggregatesService, breakdownsService, scoresService, activeTab } =
        this.getProperties('context', 'selectedUrns', 'entitiesService', 'timeseriesService', 'aggregatesService', 'breakdownsService', 'scoresService', 'activeTab');

      if (!context || !selectedUrns) {
        return;
      }

      // entities
      const entitiesUrns = new Set([...selectedUrns, ...context.urns, ...context.anomalyUrns]);
      entitiesService.request(context, entitiesUrns);


      // timeseries
      timeseriesService.request(context, selectedUrns);

      // breakdowns
      if (activeTab === ROOTCAUSE_TAB_DIMENSIONS) {
        const metricUrns = new Set(filterPrefix(context.urns, 'thirdeye:metric:'));
        const currentUrns = [...metricUrns].map(toCurrentUrn);
        const baselineUrns = [...metricUrns].map(toBaselineUrn);
        breakdownsService.request(context, new Set(currentUrns.concat(baselineUrns)));
      }

      // aggregates
      const aggregatesUrns = new Set();

      if (activeTab === ROOTCAUSE_TAB_METRICS) {
        // cache may be stale, fetch directly from service
        const entities = this.get('entitiesService.entities');
        filterPrefix(Object.keys(entities), 'thirdeye:metric:').forEach(urn => aggregatesUrns.add(urn));
      }

      if (context.anomalyUrns.size > 0) {
        filterPrefix(context.anomalyUrns, 'thirdeye:metric:').forEach(urn => aggregatesUrns.add(urn));
      }

      const offsets = ['current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];
      const offsetUrns = [...aggregatesUrns]
        .map(urn => [].concat(offsets.map(offset => toOffsetUrn(urn, offset))))
        .reduce((agg, l) => agg.concat(l), []);

      aggregatesService.request(context, new Set(offsetUrns));
      
      // scores
      const scoresUrns = aggregatesUrns;
      
      scoresService.request(context, new Set(scoresUrns));
    }
  ),

  //
  // Public properties (computed)
  //

  /**
   * Subscribed entities cache
   */
  entities: Ember.computed.reads('entitiesService.entities'),

  /**
   * Subscribed timeseries cache
   */
  timeseries: Ember.computed.reads('timeseriesService.timeseries'),

  /**
   * Subscribed aggregates cache
   */
  aggregates: Ember.computed.reads('aggregatesService.aggregates'),

  /**
   * Subscribed breakdowns cache
   */
  breakdowns: Ember.computed.reads('breakdownsService.breakdowns'),

  /**
   * Subscribed scores cache
   */
  scores: Ember.computed.reads('scoresService.scores'),

  /**
   * Primary metric urn for rootcause search
   */
  metricUrn: Ember.computed(
    'context',
    function () {
      const { context } = this.getProperties('context');
      const metricUrns = filterPrefix(context.urns, 'thirdeye:metric:');

      if (!metricUrns) { return false; }

      return metricUrns[0];
    }
  ),

  /**
   * Visible series and events in timeseries chart
   */
  chartSelectedUrns: Ember.computed(
    'entities',
    'selectedUrns',
    'invisibleUrns',
    function () {
      const { selectedUrns, invisibleUrns } =
        this.getProperties('selectedUrns', 'invisibleUrns');

      const urns = new Set(selectedUrns);
      [...invisibleUrns].forEach(urn => urns.delete(urn));

      return urns;
    }
  ),

  /**
   * (Event) entities for event table as filtered by the side bar
   */
  eventTableEntities: Ember.computed(
    'entities',
    'filteredUrns',
    function () {
      const { entities, filteredUrns } = this.getProperties('entities', 'filteredUrns');
      return filterObject(entities, (e) => filteredUrns.has(e.urn));
    }
  ),

  /**
   * Columns config for event table
   */
  eventTableColumns: EVENT_TABLE_COLUMNS,

  /**
   * (Event) entities for filtering in the side bar
   */
  eventFilterEntities: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterObject(entities, (e) => e.type == 'event');
    }
  ),

  /**
   * Visible entities for tooltip
   */
  tooltipEntities: Ember.computed(
    'entities',
    'invisibleUrns',
    'hoverUrns',
    function () {
      const { entities, invisibleUrns, hoverUrns } = this.getProperties('entities', 'invisibleUrns', 'hoverUrns');
      const visibleUrns = [...hoverUrns].filter(urn => !invisibleUrns.has(urn));
      return filterObject(entities, (e) => visibleUrns.has(e.urn));
    }
  ),

  //
  // loading indicators
  //
  isLoadingEntities: Ember.computed.gt('entitiesService.pending.size', 0),

  isLoadingTimeseries: Ember.computed.gt('timeseriesService.pending.size', 0),

  isLoadingAggregates: Ember.computed.gt('aggregatesService.pending.size', 0),

  isLoadingBreakdowns: Ember.computed.gt('breakdownsService.pending.size', 0),
  
  isLoadingScores: Ember.computed.gt('scoresService.pending.size', 0),
  
  loadingFrameworks: Ember.computed.reads('entitiesService.pending'),
  
  //
  // error indicators
  //
  hasErrorsRoute: Ember.computed.gt('routeErrors.size', 0),

  hasErrorsEntities: Ember.computed.gt('entitiesService.errors.size', 0),

  hasErrorsTimeseries: Ember.computed.gt('timeseriesService.errors.size', 0),

  hasErrorsAggregates: Ember.computed.gt('aggregatesService.errors.size', 0),

  hasErrorsBreakdowns: Ember.computed.gt('breakdownsService.errors.size', 0),

  hasErrorsScores: Ember.computed.gt('scoresService.errors.size', 0),

  hasServiceErrors: Ember.computed.or(
    'hasErrorsEntities',
    'hasErrorsTimeseries',
    'hasErrorsAggregates',
    'hasErrorsBreakdowns',
    'hasErrorsScores'
  ),

  //
  // session handling
  //
  sessionCanSave: Ember.computed(
    'sessionPermissions',
    'sessionOwner',
    'username',
    function () {
      const { sessionOwner, sessionPermissions, username } =
        this.getProperties('sessionOwner', 'sessionPermissions', 'username');

      if (sessionPermissions === ROOTCAUSE_SESSION_PERMISSIONS_READ_WRITE) {
        return true;
      }
      return sessionOwner === username;
    }
  ),

  sessionCanCopy: Ember.computed(
    'sessionId',
    'sessionPermissions',
    'sessionOwner',
    'username',
    function () {
      const { sessionId, sessionOwner, sessionPermissions, username } =
        this.getProperties('sessionId', 'sessionOwner', 'sessionPermissions', 'username');

      // NOTE: these conditions are temporary until full design for session copy is available

      if (_.isEmpty(sessionId)) { return false; }

      if (sessionOwner === username) { return false; } // temporary

      if (sessionPermissions === ROOTCAUSE_SESSION_PERMISSIONS_READ) { return true; }

      return false; // temporary
    }
  ), // Ember.computed.bool('sessionId') - when enabled

  /**
   * Sets the transient rca session properties after saving
   *
   * @param sessionId rca session id
   * @private
   */
  _updateSession(sessionId) {
    const { username } = this.getProperties('username');

    this.setProperties({
      sessionId,
      sessionUpdatedBy: username,
      sessionUpdatedTime: moment().valueOf(),
      sessionModified: false
    });
    this.transitionToRoute({ queryParams: { sessionId, anomalyId: null, metricId: null }});
  },

  /**
   * Serializes the current controller state for persistence as rca session
   *
   * @returns serialized rca session state
   * @private
   */
  _makeSession() {
    const { context, selectedUrns, sessionId, sessionName, sessionText, sessionOwner, sessionPermissions } =
      this.getProperties('context', 'selectedUrns', 'sessionId', 'sessionName', 'sessionText', 'sessionOwner', 'sessionPermissions');

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
      selectedUrns
    };
  },

  /**
   * Fetches the current session state from the backend and issues a notification if it has been updated
   *
   * @private
   */
  _checkSession() {
    const { sessionId, sessionUpdatedTime, sessionService } =
      this.getProperties('sessionId', 'sessionUpdatedTime', 'sessionService');

    if (!sessionId) { return; }

    sessionService
      .loadAsync(sessionId)
      .then((res) => {
        if (res.updated > sessionUpdatedTime) {
          this.setProperties({
            sessionUpdateWarning: `This investigation (${sessionId}) was updated by ${res.updatedBy} on ${moment(res.updated).format(dateFormatFull)}. Please refresh the page.`
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
    const { sessionId } = this.getProperties('sessionId');

    // debounce: do not run if destroyed
    if (this.isDestroyed) { return; }

    Ember.run.debounce(this, this._onCheckSessionTimer, ROOTCAUSE_SESSION_TIMER_INTERVAL);

    if (!sessionId) { return; }

    this._checkSession();
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
      const { selectedUrns } = this.getProperties('selectedUrns');
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => selectedUrns.add(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => selectedUrns.delete(urn));

      this.setProperties({
        selectedUrns: new Set(selectedUrns),
        sessionModified: true
      });
    },

    /**
     * Closure action passed into the legend component
     * to handle the hover interactivity
     * @param {String} urn
     */
    onLegendHover(urn) {
      this.set('focusedUrn', urn);
    },

    /**
     * Updates visible urns.
     *
     * @param {object} updates dictionary with urns to show and hide (true shows, false hides, omitted keys are left as is)
     * @returns {undefined}
     */
    onVisibility(updates) {
      const { invisibleUrns } = this.getProperties('invisibleUrns');
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => invisibleUrns.delete(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => invisibleUrns.add(urn));

      this.setProperties({ invisibleUrns: new Set(invisibleUrns) });
    },

    /**
     * Sets the rootcause search context
     *
     * @param {Object} context new context
     * @returns {undefined}
     */
    onContext(context) {
      this.setProperties({ context, sessionModified: true });
    },

    /**
     * Sets the urns to be displayed in the (event) entity table
     *
     * @param {Iterable} urns filtered urns to be displayed
     * @returns {undefined}
     */
    onFilter(urns) {
      this.setProperties({ filteredUrns: new Set(urns) });
    },

    /**
     * Sets the display mode for timeseries
     *
     * @param {String} timeseriesMode
     * @returns {undefined}
     */
    onChart(timeseriesMode) {
      this.setProperties({ timeseriesMode });
    },

    /**
     * Sets the hover selection for the chart tooltip
     *
     * @param {Iterable} urns urns hovered over
     * @param {Int} timestamp hover timestamp
     * @returns {undefined}
     */
    chartOnHover(urns, timestamp) {
      this.setProperties({
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
      this.setProperties({
        sessionName: name,
        sessionText: text,
        sessionModified: true
      });
    },

    /**
     * Saves the session to the backend. Overrides existing session, if any.
     *
     * @returns {undefined}
     */
    onSessionSave() {
      const { sessionService, sessionCanSave } = this.getProperties('sessionService', 'sessionCanSave');

      if (sessionCanSave) {
        const session = this._makeSession();

        return sessionService
          .saveAsync(session)
          .then(sessionId => this._updateSession(sessionId))
          .catch(() => {
            const { routeErrors } = this.getProperties('routeErrors');
            routeErrors.add('Could not save investigation');
            this.setProperties({ routeErrors: new Set(routeErrors) });
          });
      }
    },

    /**
     * Saves a dedicated copy the session to the backend and updates the session id.
     *
     * @returns {undefined}
     */
    onSessionCopy() {
      const { sessionId, sessionName, sessionService, sessionCanCopy } = this.getProperties('sessionId', 'sessionName', 'sessionService', 'sessionCanCopy');

      if (sessionCanCopy) {
        this.set('sessionName', `Copy of ${sessionName}`);

        const session = this._makeSession();

        // copy, reference old session
        delete session['id'];
        session['previousId'] = sessionId;

        return sessionService
          .saveAsync(session)
          .then(sessionId => this._updateSession(sessionId))
          .catch(() => {
            const { routeErrors } = this.getProperties('routeErrors');
            routeErrors.add('Could not copy investigation');
            this.setProperties({ routeErrors: new Set(routeErrors) });
          });
      }
    },

    /**
     * Saves the anomaly feedback o the backend. Overrides existing feedback, if any.
     *
     * @param {String} anomalyUrn anomaly entity urn
     * @param {String} feedback anomaly feedback type string
     * @param {String} comment anomaly comment
     */
    onFeedback(anomalyUrn, feedback, comment) {
      const id = anomalyUrn.split(':')[3];
      const jsonString = JSON.stringify({ feedbackType: feedback, comment });

      return fetch(`/dashboard/anomaly-merged-result/feedback/${id}`, { method: 'POST', body: jsonString });
    },

    /**
     * Selects a new primary urn for the search context.
     *
     * @param {object} updates (see onSelection, extracts "thirdeye:metric:" only)
     * @returns {undefined}
     */
    onPrimaryChange(updates) {
      const { context } = this.getProperties('context');

      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');
      const nonMetricUrns = [...context.urns].filter(urn => !urn.startsWith('thirdeye:metric:'));

      const newContext = Object.assign({}, context, { urns: new Set([...nonMetricUrns, ...metricUrns]) });

      this.send('onContext', newContext);
    },

    /**
     * Updates selected urns by adding the current primary metric.
     *
     * @returns {undefined}
     */
    onPrimarySelection() {
      const { context } = this.getProperties('context');

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
      const { context } = this.getProperties('context');

      // adjust display window if necessary
      let analysisRange = [...context.analysisRange];
      if (analysisRange[0] >= start) {
        analysisRange[0] = moment(start).startOf('day').valueOf();
      }
      if (analysisRange[1] <= end) {
        analysisRange[1] = moment(end).startOf('day').add(1, 'days').valueOf();
      }

      const newContext = Object.assign({}, context, {
        anomalyRange: [start, end],
        analysisRange,
        compareMode
      });

      this.send('onContext', newContext);
    },

    /**
     * Updates selected urns for the heatmap (appends selected filters as tail).
     * @see onSelection(updates)
     *
     * @param {Object} updates
     * @returns {undefined}
     */
    heatmapOnSelection(updates) {
      const { context } = this.getProperties('context');

      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');
      const nonMetricUrns = [...context.urns].filter(urn => !urn.startsWith('thirdeye:metric:'));

      const filters = toFilters(Object.keys(updates));
      const newMetricUrns = metricUrns.map(urn => appendFilters(urn, filters));

      const newContext = Object.assign({}, context, { urns: new Set([...nonMetricUrns, ...newMetricUrns]) });

      this.send('onContext', newContext);
    },

    /**
     * Clears error logs of data services and/or route
     */
    clearErrors(type) {
      const { entitiesService, timeseriesService, aggregatesService, breakdownsService } =
        this.getProperties('entitiesService', 'timeseriesService', 'aggregatesService', 'breakdownsService');

      switch(type) {
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

        case ROOTCAUSE_SERVICE_ROUTE:
          this.setProperties({ routeErrors: new Set() });
          break;

      }
    }
  }
});

