import Ember from 'ember';
import { filterObject, filterPrefix, toBaselineUrn, toCurrentUrn, toColor, checkStatus, toFilters, appendFilters } from 'thirdeye-frontend/helpers/utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/mocks/eventTableColumns';
import config from 'thirdeye-frontend/mocks/filterBarConfig';
import CryptoJS from 'cryptojs';
import _ from 'lodash';

const ROOTCAUSE_TAB_DIMENSIONS = "dimensions";
const ROOTCAUSE_TAB_METRICS = "metrics";
const ROOTCAUSE_TAB_EVENTS = "events";

const ROOTCAUSE_UNDO_MERGE_TIMEOUT = 1000; // seconds

// TODO: Update module import to comply by new Ember standards

export default Ember.Controller.extend({
  queryParams: [
    'metricId',
    'anomalyId',
    'sessionId'
  ],

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

  undoLog: null, // []

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
      activeTab: ROOTCAUSE_TAB_DIMENSIONS,
      timeseriesMode: 'absolute',
      undoLog: []
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

      entitiesService.request(context, selectedUrns);
      timeseriesService.request(context, selectedUrns);

      if (activeTab === ROOTCAUSE_TAB_DIMENSIONS) {
        const metricUrns = new Set(filterPrefix(context.urns, 'thirdeye:metric:'));
        const currentUrns = [...metricUrns].map(toCurrentUrn);
        const baselineUrns = [...metricUrns].map(toBaselineUrn);
        breakdownsService.request(context, new Set(currentUrns.concat(baselineUrns)));
      }

      if (activeTab === ROOTCAUSE_TAB_METRICS) {
        const metricUrns = new Set(filterPrefix(Object.keys(entities), 'thirdeye:metric:'));
        const currentUrns = [...metricUrns].map(toCurrentUrn);
        const baselineUrns = [...metricUrns].map(toBaselineUrn);
        aggregatesService.request(context, new Set(currentUrns.concat(baselineUrns)));
      }
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

  anomalyUrn: Ember.computed(
    'context',
    function () {
      const { context } = this.getProperties('context');
      const anomalyUrns = filterPrefix(context.urns, 'thirdeye:event:anomaly:');

      if (!anomalyUrns) { return false; }

      return anomalyUrns[0];
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

  isLoadingEntities: Ember.computed(
    'entitiesService.pending',
    function () {
      return this.get('entitiesService.pending').size > 0;
    }
  ),

  isLoadingTimeseries: Ember.computed(
    'timeseriesService.pending',
    function () {
      return this.get('timeseriesService.pending').size > 0;
    }
  ),

  isLoadingAggregates: Ember.computed(
    'aggregatesService.pending',
    function () {
      return this.get('aggregatesService.pending').size > 0;
    }
  ),

  isLoadingBreakdowns: Ember.computed(
    'breakdownsService.pending',
    function () {
      return this.get('breakdownsService.pending').size > 0;
    }
  ),

  isLoading: Ember.computed(
    'isLoadingEntities',
    'isLoadingTimeseries',
    'isLoadingAggregates',
    'isLoadingBreakdowns',
    function () {
      const { isLoadingEntities, isLoadingTimeseries, isLoadingAggregates, isLoadingBreakdowns } =
        this.getProperties('isLoadingEntities', 'isLoadingTimeseries', 'isLoadingAggregates', 'isLoadingBreakdowns');
      return isLoadingEntities || isLoadingTimeseries || isLoadingAggregates || isLoadingBreakdowns;
    }
  ),

  undoMessage: Ember.computed(
    'undoLog',
    function () {
      const { undoLog } = this.getProperties('undoLog');

      if (_.isEmpty(undoLog)) { return false; }

      return undoLog[undoLog.length - 1].message;
    }
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
      selectedUrns
    };
  },

  _setPropertiesUndo(properties, action, message) {
    const beforeImage = this.getProperties(...Object.keys(properties));
    const { undoLog } = this.getProperties('undoLog');

    const timestamp = moment().valueOf();

    // merge checkpoint if last action is the same within a time window
    if (!_.isEmpty(undoLog)) {
      const lastLog = undoLog[undoLog.length - 1];
      if (lastLog.action === action &&
          lastLog.message === message &&
          lastLog.timestamp >= timestamp - ROOTCAUSE_UNDO_MERGE_TIMEOUT) {

        const newLogEntry = Object.assign({}, lastLog, { timestamp });
        const newUndoLog = [..._.slice(undoLog, 0, undoLog.length - 1), newLogEntry];

        this.setProperties(Object.assign({}, properties, { undoLog: newUndoLog }));
        return;
      }
    }

    const logEntry = {
      beforeImage,
      timestamp,
      action,
      message
    };

    const newUndoLog = [...undoLog, logEntry];

    if (!_.isEqual(beforeImage, properties)) {
      this.setProperties(Object.assign({}, properties, { undoLog: newUndoLog }));
    }
  },

  _undoLast() {
    const { undoLog } = this.getProperties('undoLog');

    if (_.isEmpty(undoLog)) { return; }

    const { beforeImage } = undoLog[undoLog.length - 1];

    const newUndoLog = _.slice(undoLog, 0, undoLog.length - 1);

    this.setProperties(Object.assign({}, beforeImage, { undoLog: newUndoLog }));
  },

  //
  // Actions
  //

  actions: {
    /**
     * Undoes the last tracked user action.
     */
    onUndo() {
      this._undoLast();
    },

    /**
     * Updates selected urns.
     *
     * @param {object} updates dictionary with urns to add and remove (true adds, false removes, omitted keys are left as is)
     * @returns {undefined}
     */
    onSelection(updates) {
      const { selectedUrns } = this.getProperties('selectedUrns');

      const newSelectedUrns = new Set(selectedUrns);
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => newSelectedUrns.add(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => newSelectedUrns.delete(urn));

      this._setPropertiesUndo({
        selectedUrns: newSelectedUrns,
        sessionModified: true
      }, 'onSelection', 'Select metrics');
    },

    /**
     * Updates visible urns.
     *
     * @param {object} updates dictionary with urns to show and hide (true shows, false hides, omitted keys are left as is)
     * @returns {undefined}
     */
    onVisibility(updates) {
      const { invisibleUrns } = this.getProperties('invisibleUrns');

      const newInvisibleUrns = new Set(invisibleUrns);
      Object.keys(updates).filter(urn => updates[urn]).forEach(urn => newInvisibleUrns.delete(urn));
      Object.keys(updates).filter(urn => !updates[urn]).forEach(urn => newInvisibleUrns.add(urn));

      this.setProperties({ invisibleUrns: newInvisibleUrns });
    },

    /**
     * Sets the rootcause search context
     *
     * @param {Object} context new context
     * @returns {undefined}
     */
    onContext(context) {
      this._setPropertiesUndo({ context, sessionModified: true }, 'onContext', 'Update analysis context');
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
     * Updates selected urns for the heatmap (appends selected filters as tail).
     * @see onSelection(updates)
     *
     * @param {updates}
     * @returns {undefined}
     */
    heatmapOnSelection(updates) {
      const { context } = this.getProperties('context');

      const filters = toFilters(context.urns);

      const newUpdates = Object.keys(updates).reduce((agg, urn) => {
        const urnAugmented = appendFilters(urn, filters);
        agg[urnAugmented] = updates[urn];
        return agg;
      }, {});

      this.send('onSelection', newUpdates);
    },

    /**
     * Sets the session name and text
     *
     * @param {String} name session name/title
     * @param {String} text session summary
     * @returns {undefined}
     */
    onSessionChange(name, text) {
      this._setPropertiesUndo({
        sessionName: name,
        sessionText: text,
        sessionModified: true
      }, 'onSessionChange', 'Update analysis summary');
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
    }
  }
});

