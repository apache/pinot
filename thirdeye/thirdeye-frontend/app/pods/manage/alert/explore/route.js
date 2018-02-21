/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import RSVP from "rsvp";
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { later } from "@ember/runloop";
import { task, timeout } from 'ember-concurrency';
import { isPresent } from "@ember/utils";
import { set, get, setProperties, getWithDefault } from '@ember/object';
import { checkStatus, buildDateEod, toIso } from 'thirdeye-frontend/utils/utils';
import {
  enhanceAnomalies,
  toIdGroups,
  setMetricData,
  getMetricData,
  setUpTimeRangeOptions,
  getTopDimensions,
  buildMetricDataUrl,
  extractSeverity,
  getDuration
} from 'thirdeye-frontend/utils/manage-alert-utils';

/**
 * Shorthand for setting date defaults
 */
const dateFormat = 'YYYY-MM-DD';
const displayDateFormat = 'YYYY-MM-DD HH:mm';

/**
 * Basic alert page constants
 */
const DEFAULT_SEVERITY = 0.3;
const PAGINATION_DEFAULT = 10;
const DIMENSION_COUNT = 7;
const DURATION_DEFAULT = '3m';
const METRIC_DATA_COLOR = 'blue';

/**
 * Basic alert page defaults
 */
const endDateDefault = moment();
const resolutionOptions = ['All Resolutions'];
const dimensionOptions = ['All Dimensions'];
const wowOptions = ['Wow', 'Wo2W', 'Wo3W', 'Wo4W'];
const durationMap = { m:'month', d:'day', w:'week' };
const startDateDefault = buildDateEod(3, 'month').valueOf();
const baselineOptions = [{ name: 'Predicted', isActive: true }];

/**
 * Response type options for anomalies
 */
const anomalyResponseObj = [
  { name: 'Not reviewed yet',
    value: 'NO_FEEDBACK',
    status: 'Not Resolved'
  },
  { name: 'True anomaly',
    value: 'ANOMALY',
    status: 'Confirmed Anomaly'
  },
  { name: 'False alarm',
    value: 'NOT_ANOMALY',
    status: 'False Alarm'
  },
  { name: 'Confirmed - New Trend',
    value: 'ANOMALY_NEW_TREND',
    status: 'New Trend'
  }
];

/**
 * Build WoW array from basic options
 */
const newWowList = wowOptions.map((item) => {
  return { name: item, isActive: false };
});

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  if (anomalyIds.length) {
    const idGroups = toIdGroups(anomalyIds);
    const anomalyPromiseHash = idGroups.map((group, index) => {
      let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
      let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
      return RSVP.resolve(getAnomalies);
    });
    return RSVP.all(anomalyPromiseHash);
  } else {
    return RSVP.resolve([]);
  }
};

/**
 * Fetches severity scores for all anomalies
 * TODO: Move this and other shared requests to a common service
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {RSVP promise}
 */
const fetchSeverityScores = (anomalyIds) => {
  if (anomalyIds && anomalyIds.length) {
    const anomalyPromiseHash = anomalyIds.map((id) => {
      return RSVP.hash({
        id,
        score: fetch(`/dashboard/anomalies/score/${id}`).then(checkStatus)
      });
    });
    return RSVP.allSettled(anomalyPromiseHash);
  } else {
    return RSVP.resolve([]);
  }
};

/**
 * Derives start/end timestamps based on queryparams and user-selected time range with certain fall-backs/defaults
 * @param {String} bucketUnit - is requested range from an hourly or minutely metric?
 * @param {String} duration - the model's processed query parameter for duration ('1m', '2w', etc)
 * @param {String} start - the model's processed query parameter for startDate
 * @param {String} end - the model's processed query parameter for endDate
 * @returns {Object}
 */
const processRangeParams = (bucketUnit, duration, start, end) => {
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
  const baseEnd = isCustomDate ? moment(parseInt(end, 10)) : endDateDefault;

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
  replace: true
};

export default Route.extend({
  queryParams: {
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    repRunStatus: queryParamsConfig
  },

  beforeModel(transition) {
    const { duration, startDate } = transition.queryParams;

    // Default to 1 month of anomalies to show if no dates present in query params
    if (!duration || !startDate) {
      this.transitionTo({ queryParams: {
        duration: DURATION_DEFAULT,
        startDate: startDateDefault,
        endDate: endDateDefault
      }});
    }
  },

  model(params, transition) {
    const { id, alertData, jobId } = this.modelFor('manage.alert');
    if (!id) { return; }

    // Fetch saved time range
    const {
      duration = DURATION_DEFAULT,
      startDate = startDateDefault,
      endDate = endDateDefault
    } = getDuration();

    // Prepare endpoints for eval, mttd, projected metrics calls
    const dateParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${dateParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}?severity=${extractSeverity(alertData, DEFAULT_SEVERITY)}`;
    const performancePromiseHash = {
      current: fetch(`${evalUrl}&isProjected=FALSE`).then(checkStatus),
      projected: fetch(`${evalUrl}&isProjected=TRUE`).then(checkStatus),
      mttd: fetch(mttdUrl).then(checkStatus)
    };

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
          alertEvalMetrics
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
      jobId,
      startDate,
      endDate,
      duration,
      alertEvalMetrics
    } = model;

    // Pull alert properties into context
    const {
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters,
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
      filters,
      startStamp,
      endStamp,
      bucketSize,
      bucketUnit,
      baseEnd,
      baseStart,
      exploreDimensions
    };

    // Load endpoints for projected metrics. TODO: consolidate into CP if duplicating this logic
    const qsParams = `start=${baseStart.utc().format(dateFormat)}&end=${baseEnd.utc().format(dateFormat)}&useNotified=true`;
    const anomalyDataUrl = `/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=`;
    const metricsUrl = `/data/autocomplete/metric?name=${dataset}::${metricName}`;
    const anomaliesUrl = `/dashboard/anomaly-function/${alertId}/anomalies?${qsParams}`;

    const anomalyPromiseHash = {
      projectedMttd: 0, // In overview mode, no projected MTTD value is needed
      metricsByName: fetch(metricsUrl).then(checkStatus),
      anomalyIds: fetch(anomaliesUrl).then(checkStatus)
    };

    return RSVP.hash(anomalyPromiseHash)
      .then((data) => {
        const totalAnomalies = data.anomalyIds.length;
        Object.assign(model.alertEvalMetrics.projected, { mttd: data.projectedMttd });
        Object.assign(config, { id: data.metricsByName.length ? data.metricsByName.pop().id : '' });
        Object.assign(model, {
          anomalyIds: data.anomalyIds,
          exploreDimensions,
          totalAnomalies,
          anomalyDataUrl,
          anomaliesUrl,
          config
        });
        fetch(`/data/maxDataTime/metricId/${config.id}`).then(checkStatus);
      })
      // Note: In the event of custom date selection, the end date might be less than maxTime
      .then((maxTime) => {
        Object.assign(model, { metricDataUrl: buildMetricDataUrl({
          maxTime,
          id: config.id,
          filters: config.filters,
          granularity: config.bucketUnit,
          dimension: config.exploreDimensions ? config.exploreDimensions.split(',')[0] : 'All'
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
      email,
      filters,
      duration,
      config,
      loadError,
      metricDataUrl,
      anomalyDataUrl,
      topDimensionsUrl,
      exploreDimensions,
      totalAnomalies,
      alertEvalMetrics,
      allConfigGroups,
      allAppNames,
      rawAnomalyData
    } = model;

    // Prime the controller
    controller.setProperties({
      loadError,
      jobId,
      alertData,
      alertId: id,
      DEFAULT_SEVERITY,
      anomalyDataUrl,
      baselineOptions,
      anomalyResponseObj,
      alertEvalMetrics,
      anomaliesLoaded: false,
      isMetricDataInvalid: false,
      isMetricDataLoading: true,
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

    // Begin loading anomaly and graph data as concurrency tasks
    // See https://github.com/linkedin/pinot/pull/2518#discussion-diff-169751380R366
    if (jobId !== -1) {
      this.get('loadAnomalyData').perform(anomalyIds);
      this.get('loadGraphData').perform(metricDataUrl, exploreDimensions);
    }
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    // Cancel all pending concurrency tasks in controller
    if (isExiting) {
      controller.clearAll();
    }
  },

  /**
   * Fetches change rate data for each available anomaly id
   * @method fetchCombinedAnomalyChangeData
   * @param {Array} anomalyData - array of processed anomalies
   * @returns {RSVP promise}
   */
  fetchCombinedAnomalyChangeData(anomalyData) {
    let promises = {};

    anomalyData.forEach((anomaly) => {
      let id = anomaly.anomalyId;
      promises[id] = fetch(`/anomalies/${id}`).then(checkStatus);
    });

    return RSVP.hash(promises);
  },

  /**
   * Performs the repetitive task of setting graph properties based on
   * returned metric data and dimension data
   * @method setGraphProperties
   * @param {Object} metricData - returned metric timeseries data
   * @param {String} exploreDimensions - string of metric dimensions
   * @returns {RSVP promise}
   */
  setGraphProperties(metricData, exploreDimensions) {
    const alertDimension = exploreDimensions ? exploreDimensions.split(',')[0] : '';
    Object.assign(metricData, { color: METRIC_DATA_COLOR });
    this.controller.setProperties({
      metricData,
      alertDimension,
      topDimensions: [],
      isMetricDataLoading: false
    });
    // If alert has dimensions set, load them into graph once replay is done.
    if (exploreDimensions && !this.controller.isReplayPending) {
      this.controller.set('topDimensions', getTopDimensions(metricData, DIMENSION_COUNT));
    }
  },

  /**
   * Fetch all anomalies we have Ids for. Enhance the data and populate power-select filter options.
   * Using ember concurrency parent/child tasks. When parent is cancelled, so are children
   * http://ember-concurrency.com/docs/child-tasks.
   * TODO: complete concurrency task error handling and refactor child tasks for cuncurrency.
   * @param {Array} anomalyIds - the IDs of anomalies that have been reported for this alert.
   * @return {undefined}
   */
  loadAnomalyData: task(function * (anomalyIds) {
    yield timeout(300);
    // Load data for each anomaly Id
    const rawAnomalies = yield fetchCombinedAnomalies(anomalyIds);
    // Fetch and append severity score to each anomaly record
    const severityScores = yield fetchSeverityScores(anomalyIds);
    // Process anomaly records to make them template-ready
    const anomalyData = yield enhanceAnomalies(rawAnomalies, severityScores);
    // Prepare de-duped power-select option arrays
    resolutionOptions.push(...new Set(anomalyData.map(record => record.anomalyFeedback)));
    dimensionOptions.push(...new Set(anomalyData.map(anomaly => anomaly.dimensionString)));
    // Push anomaly data into controller
    this.controller.setProperties({
      anomaliesLoaded: true,
      anomalyData,
      resolutionOptions,
      dimensionOptions
    });
    // Fetch and append extra WoW data for each anomaly record
    const wowData = yield this.fetchCombinedAnomalyChangeData(anomalyData);
    anomalyData.forEach((anomaly) => {
      anomaly.wowData = wowData[anomaly.anomalyId] || {};
    });
    // Load enhanced dataset into controller (WoW options will appear)
    this.controller.setProperties({
      anomalyData,
      baselineOptionsLoading: false,
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
    yield timeout(300);
    const metricId = getWithDefault(this, 'currentModel.config.id', null);
    const isGraphDataLocal = metricId && localStorage.getItem(metricId) !== null;

    // Fetch and load graph metric data.
    fetch(metricDataUrl).then(checkStatus)
      .then((metricData) => {
        // Load graph with metric data from timeseries API
        this.setGraphProperties(metricData, exploreDimensions);
      })
      .catch((errors) => {
        this.controller.setProperties({
          isMetricDataInvalid: true,
          isMetricDataLoading: false,
          graphMessageText: 'Error loading metric data'
        });
      });
  }).cancelOn('deactivate').restartable(),

  actions: {
    /**
    * Refresh route's model.
    */
    refreshModel() {
      this.refresh();
    },

    /**
    * Change link state in parent controller to reflect transition to tuning route
    */
    updateParentLink() {
      setProperties(this.controllerFor('manage.alert'), {
        isOverViewModeActive: false,
        isEditModeActive: true
      });
    },

    /**
     * Handle any errors occurring in model/afterModel in parent route
     * https://www.emberjs.com/api/ember/2.16/classes/Route/events/error?anchor=error
     * https://guides.emberjs.com/v2.18.0/routing/loading-and-error-substates/#toc_the-code-error-code-event
     */
    error(error, transition) {
      return true;
    }
  }
});
