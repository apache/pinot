/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { isArray } from '@ember/array';
import { later } from '@ember/runloop';
import { task, timeout } from 'ember-concurrency';
import { inject as service } from '@ember/service';
import {
  set,
  get,
  setProperties,
  getWithDefault
} from '@ember/object';
import { isPresent, isNone, isBlank } from '@ember/utils';
import {
  checkStatus,
  buildDateEod,
  makeFilterString,
  toIso
} from 'thirdeye-frontend/utils/utils';
import {
  enhanceAnomalies,
  toIdGroups,
  setUpTimeRangeOptions,
  getTopDimensions,
  buildMetricDataUrl,
  extractSeverity
} from 'thirdeye-frontend/utils/manage-alert-utils';
import {
  selfServeApiCommon,
  selfServeApiGraph
} from 'thirdeye-frontend/utils/api/self-serve';
import {
  anomalyResponseObj,
  anomalyResponseMap
} from 'thirdeye-frontend/utils/anomaly';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';

/**
 * Shorthand for setting date defaults
 */
const dateFormat = 'YYYY-MM-DD';
const displayDateFormat = 'YYYY-MM-DD HH:mm';

/**
 * Basic alert page constants
 */
const DEFAULT_SEVERITY = 0.3;
const DIMENSION_COUNT = 7;
const METRIC_DATA_COLOR = 'blue';

/**
 * Basic alert page defaults
 */
const wowOptions = ['Wow', 'Wo2W', 'Wo3W', 'Wo4W'];
const durationMap = { m:'month', d:'day', w:'week' };
const baselineOptions = [{ name: 'Predicted', isActive: true }];
const defaultDurationObj = {
  duration: '3m',
  startDate: buildDateEod(3, 'month').valueOf(),
  endDate: moment()
};

/**
 * Build WoW array from basic options
 */
const newWowList = wowOptions.map((item) => {
  return { name: item, isActive: false };
});

/**
 * Derives start/end timestamps based on queryparams and user-selected time range with certain fall-backs/defaults
 * @param {String} bucketUnit - is requested range from an hourly or minutely metric?
 * @param {String} duration - the model's processed query parameter for duration ('1m', '2w', etc)
 * @param {String} start - the model's processed query parameter for startDate
 * @param {String} end - the model's processed query parameter for endDate
 * @returns {Object}
 */
const processRangeParams = (bucketUnit = 'DAYS', duration, start, end) => {
  // To avoid loading too much data, override our time span defaults based on whether the metric is 'minutely'
  const isMetricMinutely = bucketUnit.toLowerCase().includes('minute');
  const defaultQueryUnit = isMetricMinutely ? 'week' : 'month';
  const defaultQuerySize = isMetricMinutely ? 2 : 1;

  // We also allow a 'duration' query param to set the time range. For example, duration=15d (last 15 days)
  const qsRegexMatch = duration.match(new RegExp(/^(\d)+([d|m|w])$/i));
  const durationMatch = duration && qsRegexMatch ? qsRegexMatch : [];

  // If the duration string is recognized, we use it. Otherwise, we fall back on the defaults above
  const querySize = durationMatch && durationMatch.length ? durationMatch[1] : defaultQuerySize;
  const queryUnit = durationMatch && durationMatch.length ? durationMap[durationMatch[2].toLowerCase()] : defaultQueryUnit;

  // If duration = 'custom', we know the user is requesting specific start/end times.
  // In this case, we will use those instead of our parsed duration & defaults
  const isCustomDate = duration === 'custom';
  const baseStart = isCustomDate ? moment(parseInt(start, 10)) : buildDateEod(querySize, queryUnit);
  const baseEnd = isCustomDate ? moment(parseInt(end, 10)) : moment();

  // These resulting timestamps are used for our graph and anomaly queries
  const startStamp = baseStart.valueOf();
  const endStamp = baseEnd.valueOf();

  return { startStamp, endStamp, baseStart, baseEnd };
};

/**
 * Setup for query param behavior
 */
const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Route.extend({
  queryParams: {
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    openReport: queryParamsConfig,
    repRunStatus: queryParamsConfig
  },

  /**
   * Make duration service accessible
   */
  durationCache: service('services/duration'),
  session: service(),

  beforeModel(transition) {
    const { duration, startDate } = transition.queryParams;
    // Default to 1 month of anomalies to show if no dates present in query params
    if (!duration || !startDate) {
      this.transitionTo({ queryParams: defaultDurationObj });
    }
  },

  model(params, transition) {
    const { id, alertData, jobId } = this.modelFor('manage.alert');
    const isReplayDone = isNone(jobId) && jobId !== -1;
    if (!id) { return; }

    // Get duration data from service
    const {
      duration,
      startDate,
      endDate
    } = this.get('durationCache').getDuration(transition.queryParams, defaultDurationObj);

    // Prepare endpoints for eval, mttd, projected metrics calls
    const dateParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${dateParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}?severity=${extractSeverity(alertData, DEFAULT_SEVERITY)}`;
    let performancePromiseHash = {
      current: {},
      projected: {},
      mttd: ''
    };

    // Once replay is done or timed out, this route loads all needed data. We load placeholders first.
    if (isReplayDone) {
      performancePromiseHash = {
        current: fetch(`${evalUrl}&isProjected=FALSE`).then(checkStatus),
        projected: fetch(`${evalUrl}&isProjected=TRUE`).then(checkStatus),
        mttd: fetch(mttdUrl).then(checkStatus)
      };
    }

    return RSVP.hash(performancePromiseHash)
      .then((alertEvalMetrics) => {
        Object.assign(alertEvalMetrics.current, { mttd: alertEvalMetrics.mttd});
        return {
          id,
          jobId,
          alertData,
          duration,
          startDate,
          evalUrl,
          endDate,
          dateParams,
          alertEvalMetrics,
          isReplayDone
        };
      })
      // Catch is not mandatory here due to our error action, but left it to add more context.
      .catch((error) => {
        return RSVP.reject({ error, location: `${this.routeName}:model`, calls: performancePromiseHash });
      });
  },

  afterModel(model) {
    this._super(model);

    const {
      id: alertId,
      alertData,
      isReplayDone,
      startDate,
      endDate,
      duration,
      dateParams,
      alertEvalMetrics
    } = model;

    // Pull alert properties into context
    const {
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters: filtersRaw,
      bucketSize,
      bucketUnit
    } = alertData;

    // Derive start/end time ranges based on querystring input with fallback on default '1 month'
    const {
      startStamp,
      endStamp,
      baseStart,
      baseEnd
    } = processRangeParams(bucketUnit, duration, startDate, endDate);

    // Set initial value for metricId for early transition cases
    const config = {
      startStamp,
      endStamp,
      bucketSize,
      bucketUnit,
      baseEnd,
      baseStart,
      exploreDimensions,
      filters: filtersRaw ? makeFilterString(filtersRaw) : ''
    };

    // Load endpoints for projected metrics. TODO: consolidate into CP if duplicating this logic
    const anomalyDataUrl = getAnomalyDataUrl(startStamp, endStamp);
    const metricsUrl = selfServeApiCommon.metricAutoComplete(metricName);
    const anomaliesUrl = `/dashboard/anomaly-function/${alertId}/anomalies?${dateParams}&useNotified=true`;
    let anomalyPromiseHash = {
      projectedMttd: 0,
      metricsByName: [],
      anomalyIds: []
    };

    // If replay still pending, load placeholders for this data.
    if (isReplayDone) {
      anomalyPromiseHash = {
        projectedMttd: 0, // In overview mode, no projected MTTD value is needed
        metricsByName: fetch(metricsUrl).then(checkStatus),
        anomalyIds: fetch(anomaliesUrl).then(checkStatus)
      };
    }

    return RSVP.hash(anomalyPromiseHash)
      .then(async (data) => {
        const metricId = this._locateMetricId(data.metricsByName, alertData);
        const totalAnomalies = data.anomalyIds.length;
        Object.assign(alertEvalMetrics.projected, { mttd: data.projectedMttd });
        Object.assign(config, { id: metricId });
        Object.assign(model, {
          anomalyIds: data.anomalyIds,
          exploreDimensions,
          totalAnomalies,
          anomalyDataUrl,
          anomaliesUrl,
          config
        });
        const maxTimeUrl = selfServeApiGraph.maxDataTime(metricId);
        const maxTime = isReplayDone && metricId ? await fetch(maxTimeUrl).then(checkStatus) : moment().valueOf();
        Object.assign(model, { metricDataUrl: buildMetricDataUrl({
          maxTime,
          endStamp: config.endStamp,
          startStamp: config.startStamp,
          id: metricId,
          filters: config.filters,
          granularity: config.bucketUnit,
          dimension: 'All' // NOTE: avoid dimension explosion - config.exploreDimensions ? config.exploreDimensions.split(',')[0] : 'All'
        })});
      })
      // Catch is not mandatory here due to our error action, but left it to add more context
      .catch((err) => {
        return RSVP.reject({ err, location: `${this.routeName}:afterModel`, calls: anomalyPromiseHash });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      jobId,
      alertData,
      anomalyIds,
      duration,
      config,
      loadError,
      isReplayDone,
      metricDataUrl,
      anomalyDataUrl,
      totalAnomalies,
      exploreDimensions,
      alertEvalMetrics
    } = model;

    // Prime the controller
    controller.setProperties({
      loadError,
      jobId,
      alertData,
      alertId: id,
      DEFAULT_SEVERITY,
      totalAnomalies,
      anomalyDataUrl,
      baselineOptions,
      alertEvalMetrics,
      anomaliesLoaded: false,
      isMetricDataInvalid: false,
      isMetricDataLoading: true,
      alertDimension: exploreDimensions,
      isReplayPending: isPresent(jobId) && jobId !== -1,
      alertHasDimensions: isPresent(exploreDimensions),
      timeRangeOptions: setUpTimeRangeOptions(['3m'], duration),
      baselineOptionsLoading: anomalyIds && anomalyIds.length > 0,
      responseOptions: anomalyResponseObj.map(response => response.name)
    });

    // Kick off controller defaults and replay status check
    controller.initialize();

    // Ensure date range picker gets populated correctly
    later(this, () => {
      controller.setProperties({
        activeRangeStart: moment(config.startStamp).format(displayDateFormat),
        activeRangeEnd: moment(config.endStamp).format(displayDateFormat)
      });
    });

    // Once replay is finished, begin loading anomaly and graph data as concurrency tasks
    // See https://github.com/linkedin/pinot/pull/2518#discussion-diff-169751380R366
    if (isReplayDone) {
      get(this, 'loadAnomalyData').perform(anomalyIds, exploreDimensions);
      get(this, 'loadGraphData').perform(metricDataUrl, exploreDimensions);
    }
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    // Cancel all pending concurrency tasks in controller
    if (isExiting) {
      get(this, 'loadAnomalyData').cancelAll();
      get(this, 'loadGraphData').cancelAll();
      controller.clearAll();
    }
  },

  /**
   * Performs the repetitive task of setting graph properties based on
   * returned metric data and dimension data
   * @method _setGraphProperties
   * @param {Object} metricData - returned metric timeseries data
   * @param {String} exploreDimensions - string of metric dimensions
   * @returns {undefined}
   * @private
   */
  _setGraphProperties(metricData, exploreDimensions) {
    const alertDimension = exploreDimensions ? exploreDimensions.split(',')[0] : '';
    Object.assign(metricData, { color: METRIC_DATA_COLOR });
    this.controller.setProperties({
      metricData,
      alertDimension,
      isMetricDataLoading: false
    });
    // If alert has dimensions set, load them into graph once replay is done.
    if (exploreDimensions && !this.controller.isReplayPending) {
      const topDimensions = getTopDimensions(metricData, DIMENSION_COUNT);
      this.controller.setProperties({
        topDimensions,
        isDimensionFetchDone: true,
        availableDimensions: topDimensions.length
      });
    }
  },

  /**
   * Tries find a specific metric id based on a common dataset string
   * @method _locateMetricId
   * @param {Array} metricList - list of metrics from metric-by-name lookup
   * @param {Object} alertData - currently loaded alert properties
   * @returns {Number} target metric id
   * @private
   */
  _locateMetricId(metricList, alertData) {
    const metricId = metricList.find((metric) => {
      return (metric.name === alertData.metric) && (metric.dataset === alertData.collection);
    }) || { id: 0 };
    return isBlank(metricList) ? 0 : metricId.id;
  },

  /**
   * Returns an aggregate list of all labels found in the currently-loaded anomaly set
   * @method _filterResolutionLabels
   * @param {Array} anomalyData - list of all anomalies for current alert
   * @returns {Array} list of all labels found in anomaly set
   * @private
   */
  _filterResolutionLabels(anomalyData) {
    let availableLabels = [];
    anomalyData.forEach((anomaly) => {
      let mappedLabel = anomalyResponseMap[anomaly.anomalyFeedback];
      if (mappedLabel) { availableLabels.push(mappedLabel); }
    });
    return availableLabels;
  },

  /**
   * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
   * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
   * @param {Array} anomalyIds - list of all found anomaly ids
   * @returns {RSVP promise}
   */
  fetchCombinedAnomalies: task(function * (anomalyIds) {
    yield timeout(300);
    if (anomalyIds.length) {
      const idGroups = toIdGroups(anomalyIds);
      const anomalyPromiseHash = idGroups.map((group, index) => {
        let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
        let url = `/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`;
        let getAnomalies = get(this, 'fetchAnomalyEntity').perform(url);
        return RSVP.resolve(getAnomalies);
      });
      return RSVP.all(anomalyPromiseHash);
    } else {
      return RSVP.resolve([]);
    }
  }),

  /**
   * Fetches change rate data for each available anomaly id
   * @method fetchCombinedAnomalyChangeData
   * @param {Array} anomalyData - array of processed anomalies
   * @returns {RSVP promise}
   */
  fetchCombinedAnomalyChangeData: task(function * (anomalyData) {
    yield timeout(300);
    let promises = [];

    anomalyData.forEach((anomaly) => {
      let id = anomaly.anomalyId;
      promises[id] = get(this, 'fetchAnomalyEntity').perform(`/anomalies/${id}`);
    });

    return RSVP.hash(promises);
  }),

  /**
   * Fetches severity scores for all anomalies
   * TODO: Move this and other shared requests to a common service
   * @param {Array} anomalyIds - list of all found anomaly ids
   * @returns {RSVP promise}
   */
  fetchSeverityScores: task(function * (anomalyIds) {
    yield timeout(300);
    if (anomalyIds && anomalyIds.length) {
      const anomalyPromiseHash = anomalyIds.map((id) => {
        return RSVP.hash({
          id,
          score: get(this, 'fetchAnomalyEntity').perform(`/dashboard/anomalies/score/${id}`)
        });
      });
      return RSVP.allSettled(anomalyPromiseHash);
    } else {
      return RSVP.resolve([]);
    }
  }),

  /**
   * Fetch any single entity as a cancellable concurrency task
   * @param {String} url - endpoint for fetch
   * @returns {fetch promise}
   */
  fetchAnomalyEntity: task(function * (url) {
    yield timeout(300);
    return fetch(url).then(checkStatus);
  }),

  /**
   * Fetch all anomalies we have Ids for. Enhance the data and populate power-select filter options.
   * Using ember concurrency parent/child tasks. When parent is cancelled, so are children
   * http://ember-concurrency.com/docs/child-tasks.
   * TODO: complete concurrency task error handling and refactor child tasks for cuncurrency.
   * @param {Array} anomalyIds - the IDs of anomalies that have been reported for this alert.
   * @return {undefined}
   */
  loadAnomalyData: task(function * (anomalyIds, exploreDimensions) {
    const dimensionOptions = ['All Dimensions'];
    const hasDimensions = exploreDimensions && exploreDimensions.length;
    // Load data for each anomaly Id
    const rawAnomalies = yield get(this, 'fetchCombinedAnomalies').perform(anomalyIds);
    // Fetch and append severity score to each anomaly record
    const severityScores = yield get(this, 'fetchSeverityScores').perform(anomalyIds);
    // Process anomaly records to make them template-ready
    const anomalyData = yield enhanceAnomalies(rawAnomalies, severityScores);
    // Prepare de-duped power-select option array for anomaly feedback
    const resolutionOptions = ['All Resolutions', ...new Set(this._filterResolutionLabels(anomalyData))];
    // Populate dimensions power-select options if dimensions exist
    if (hasDimensions) {
      dimensionOptions.push(...new Set(anomalyData.map(anomaly => anomaly.dimensionString)));
    }
    // Push anomaly data into controller
    this.controller.setProperties({
      anomalyData,
      dimensionOptions,
      resolutionOptions,
      anomaliesLoaded: true,
      totalLoadedAnomalies: anomalyData.length,
      baselineOptionsLoading: false
    });
    // Fetch and append extra WoW data for each anomaly record
    const wowData = yield get(this, 'fetchCombinedAnomalyChangeData').perform(anomalyData);
    anomalyData.forEach((anomaly) => {
      anomaly.wowData = wowData[anomaly.anomalyId] || {};
    });
    // Load enhanced dataset into controller (WoW options will appear)
    this.controller.setProperties({
      anomalyData,
      baselineOptions: [baselineOptions[0], ...newWowList]
    });
  // We use .cancelOn('deactivate') to make sure the task cancels when the user leaves the route.
  // We use restartable to ensure that only one instance of the task is running at a time, hence
  // any time setupController performs the task, any prior instances are canceled.
  }).cancelOn('deactivate').restartable(),

  /**
   * Concurrenty task to ping the job-info endpoint to check status of an ongoing replay job.
   * If there is no progress after a set time, we display an error message.
   * @param {Number} jobId - the id for the newly triggered replay job
   * @param {String} functionName - user-provided new function name (used to validate creation)
   * @return {undefined}
   */
  loadGraphData: task(function * (metricDataUrl, exploreDimensions) {
    try {
      // Fetch and load graph metric data from either local store or API
      const metricData = yield fetch(metricDataUrl).then(checkStatus);
      // Load graph with metric data from timeseries API
      yield this._setGraphProperties(metricData, exploreDimensions);
    } catch (e) {
      this.controller.setProperties({
        isMetricDataInvalid: true,
        isMetricDataLoading: false
      });
    }
  }).cancelOn('deactivate').restartable(),

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

    /**
    * Refresh route's model.
    */
    refreshModel() {
      this.replaceWith({ queryParams: { openReport: false } });
    },

    /**
    * Refresh anomaly data when changes are made
    */
    refreshAnomalyTable() {
      const { anomalyIds, exploreDimensions } = this.currentModel;
      if (anomalyIds && anomalyIds.length) {
        get(this, 'loadAnomalyData').perform(anomalyIds, exploreDimensions);
      }
    },

    /**
    * Change link state in parent controller to reflect transition to tuning route
    */
    updateParentLink() {
      setProperties(this.controllerFor('manage.alert'), {
        isOverViewModeActive: false,
        isEditModeActive: true
      });
      // Cancel route's main concurrency tasks
      get(this, 'loadAnomalyData').cancelAll();
      get(this, 'loadGraphData').cancelAll();
    },

    /**
     * Handle any errors occurring in model/afterModel in parent route
     * https://www.emberjs.com/api/ember/2.16/classes/Route/events/error?anchor=error
     * https://guides.emberjs.com/v2.18.0/routing/loading-and-error-substates/#toc_the-code-error-code-event
     */
    error(error) {
      return true;
    }
  }
});
