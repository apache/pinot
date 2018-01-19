import Ember from 'ember';
import { filterObject, filterPrefix, toBaselineUrn, toCurrentUrn, toOffsetUrn, toColor, checkStatus, toFilters, appendFilters } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import config from 'thirdeye-frontend/mocks/filterBarConfig';
import _ from 'lodash';
import fetch from 'fetch';
import moment from 'moment';

const ROOTCAUSE_TAB_DIMENSIONS = "dimensions";
const ROOTCAUSE_TAB_METRICS = "metrics";
const ROOTCAUSE_TAB_EVENTS = "events";

const ROOTCAUSE_SERVICE_ENTITIES = "entities";
const ROOTCAUSE_SERVICE_TIMESERIES = "timeseries";
const ROOTCAUSE_SERVICE_AGGREGATES = "aggregates";
const ROOTCAUSE_SERVICE_BREAKDOWNS = "breakdowns";

// TODO: Update module import to comply by new Ember standards

export default Ember.Controller.extend({
  session: Ember.inject.service(),

  /**
   * User ldap
   * @type {String}
   */
  user: Ember.computed.reads('session.data.authenticated.name'),

  queryParams: [
    'metricId',
    'anomalyId',
    'sessionId'
  ],

  //
  // route errors
  //
  routeErrors: null, // Set

  //
  // services
  //
  entitiesService: Ember.inject.service('rootcause-entities-cache'),

  timeseriesService: Ember.inject.service('rootcause-timeseries-cache'),

  aggregatesService: Ember.inject.service('rootcause-aggregates-cache'),

  breakdownsService: Ember.inject.service('rootcause-breakdowns-cache'),

  //
  // rootcause search context
  //
  context: null, // { urns: Set, anomalyRange: [2], baselineRange: [2], analysisRange: [2] }

  //
  // user selection
  //
  selectedUrns: null, // Set

  invisibleUrns: null, // Set

  hoverUrns: null, // Set

  filteredUrns: null,

  activeTab: null, // ""

  timeseriesMode: null, // ""

  //
  // session data
  //

  sessionName: null, // ""

  sessionText: null, // ""

  sessionModified: null, // true

  //
  // static component config
  //
  filterConfig: config, // {}

  settingsConfig: null, // {}

  init() {
    this._super(...arguments);
    this.setProperties({
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      activeTab: ROOTCAUSE_TAB_METRICS,
      timeseriesMode: 'absolute'
    });
  },

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
      const { context, entities, selectedUrns, entitiesService, timeseriesService, aggregatesService, breakdownsService, activeTab } =
        this.getProperties('context', 'entities', 'selectedUrns', 'entitiesService', 'timeseriesService', 'aggregatesService', 'breakdownsService', 'activeTab');

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
      const offsets = ['current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];
      const aggregatesUrns = new Set();

      if (activeTab === ROOTCAUSE_TAB_METRICS) {
        filterPrefix(Object.keys(entities), 'thirdeye:metric:').forEach(urn => aggregatesUrns.add(urn));
      }

      if (!_.isEmpty(context.anomalyUrns)) {
        filterPrefix(context.anomalyUrns, 'thirdeye:metric:').forEach(urn => aggregatesUrns.add(urn));
      }

      const offsetUrns = [...aggregatesUrns].map(urn => [].concat(offsets.map(offset => toOffsetUrn(urn, offset)))).reduce((agg, l) => agg.concat(l), []);
      aggregatesService.request(context, new Set(offsetUrns));
    }
  ),

  //
  // Public properties (computed)
  //

  entities: Ember.computed(
    'entitiesService.entities',
    function () {
      const entities = _.cloneDeep(this.get('entitiesService.entities'));

      Object.keys(entities).forEach(urn => entities[urn].color = toColor(urn));
      return entities;
    }
  ),

  timeseries: Ember.computed(
    'timeseriesService.timeseries',
    function () {
      return this.get('timeseriesService.timeseries');
    }
  ),

  aggregates: Ember.computed(
    'aggregatesService.aggregates',
    function () {
      return this.get('aggregatesService.aggregates');
    }
  ),

  breakdowns: Ember.computed(
    'breakdownsService.breakdowns',
    function () {
      return this.get('breakdownsService.breakdowns');
    }
  ),

  metricUrn: Ember.computed(
    'context',
    function () {
      const { context } = this.getProperties('context');
      const metricUrns = filterPrefix(context.urns, 'thirdeye:metric:');

      if (!metricUrns) { return false; }

      return metricUrns[0];
    }
  ),

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

  eventTableEntities: Ember.computed(
    'entities',
    'filteredUrns',
    function () {
      const { entities, filteredUrns } = this.getProperties('entities', 'filteredUrns');
      return filterObject(entities, (e) => filteredUrns.has(e.urn));
    }
  ),

  eventTableColumns: EVENT_TABLE_COLUMNS,

  eventFilterEntities: Ember.computed(
    'entities',
    function () {
      const { entities } = this.getProperties('entities');
      return filterObject(entities, (e) => e.type == 'event');
    }
  ),

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

  isLoadingEntities: Ember.computed.gt('entitiesService.pending.size', 0),

  isLoadingTimeseries: Ember.computed.gt('timeseriesService.pending.size', 0),

  isLoadingAggregates: Ember.computed.gt('aggregatesService.pending.size', 0),

  isLoadingBreakdowns: Ember.computed.gt('breakdownsService.pending.size', 0),

  hasErrorsRoute: Ember.computed.gt('routeErrors.size', 0),

  hasErrorsEntities: Ember.computed.gt('entitiesService.errors.size', 0),

  hasErrorsTimeseries: Ember.computed.gt('timeseriesService.errors.size', 0),

  hasErrorsAggregates: Ember.computed.gt('aggregatesService.errors.size', 0),

  hasErrorsBreakdowns: Ember.computed.gt('breakdownsService.errors.size', 0),

  hasServiceErrors: Ember.computed.or(
    'hasErrorsEntities',
    'hasErrorsTimeseries',
    'hasErrorsAggregates',
    'hasErrorsBreakdowns'
  ),

  _makeSession() {
    const { context, selectedUrns, sessionId, sessionName, sessionText } =
      this.getProperties('context', 'selectedUrns', 'sessionId', 'sessionName', 'sessionText');

    return {
      id: sessionId,
      name: sessionName,
      text: sessionText,
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
      const jsonString = JSON.stringify(this._makeSession());

      return fetch(`/session/`, { method: 'POST', body: jsonString })
        .then(checkStatus)
        .then(res => this.setProperties({
          sessionId: res,
          sessionOwner: this.get('user'),
          sessionUpdatedTime: moment().format('LLLL'),
          sessionModified: false
        }));
    },

    /**
     * Saves a dedicated copy the session to the backend and updates the session id.
     *
     * @returns {undefined}
     */
    onSessionCopy() {
      const { sessionId } = this.getProperties('sessionId');

      const session = this._makeSession();
      delete session['id'];
      session['previousId'] = sessionId;

      const jsonString = JSON.stringify(session);

      return fetch(`/session/`, { method: 'POST', body: jsonString })
        .then(checkStatus)
        .then(res => this.setProperties({
          sessionId: res,
          sessionModified: false
        }));
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

      const newContext = Object.assign({}, context, {
        anomalyRange: [start, end],
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
     * Clears error logs of data services
     */
    clearServiceErrors(type) {
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

      }
    },

    /**
     * Clears error logs of route loading/controller setup
     */
    clearRouteErrors() {
      this.setProperties({ routeErrors: new Set() });
    }
  }
});

