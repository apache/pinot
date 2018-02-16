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
import { set, get, setProperties } from '@ember/object';
import { isPresent } from "@ember/utils";
import { checkStatus, buildDateEod, toIso } from 'thirdeye-frontend/utils/utils';
import {
  enhanceAnomalies,
  toIdGroups,
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
 * Basic alert page defaults
 */
const defaultSeverity = 0.3;
const paginationDefault = 10;
const dimensionCount = 7;
const durationDefault = '3m';
const metricDataColor = 'blue';
const durationMap = { m:'month', d:'day', w:'week' };
const startDateDefault = buildDateEod(3, 'month').valueOf();
const endDateDefault = moment();

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
        duration: durationDefault,
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
      duration = durationDefault,
      startDate = startDateDefault,
      endDate = endDateDefault
    } = getDuration();

    // Prepare endpoints for eval, mttd, projected metrics calls
    const dateParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${dateParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}?severity=${extractSeverity(alertData, defaultSeverity)}`;
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

    // Initial value setup for displayed option lists
    let subD = {};
    let anomalyData = [];
    let rawAnomalies = [];
    const notCreateError = jobId !== -1;
    const resolutionOptions = ['All Resolutions'];
    const dimensionOptions = ['All Dimensions'];
    const wowOptions = ['Wow', 'Wo2W', 'Wo3W', 'Wo4W'];
    const baselineOptions = [{ name: 'Predicted', isActive: true }];
    const responseOptions = anomalyResponseObj.map(response => response.name);
    const timeRangeOptions = setUpTimeRangeOptions(['3m'], duration);
    const alertDimension = exploreDimensions ? exploreDimensions.split(',')[0] : '';
    const isReplayPending = isPresent(jobId) && jobId !== -1;
    const newWowList = wowOptions.map((item) => {
      return { name: item, isActive: false };
    });

    // Prime the controller
    controller.setProperties({
      loadError,
      jobId,
      alertData,
      alertId: id,
      defaultSeverity,
      isReplayPending,
      anomalyDataUrl,
      baselineOptions,
      responseOptions,
      timeRangeOptions,
      anomalyResponseObj,
      alertEvalMetrics,
      anomaliesLoaded: false,
      isMetricDataInvalid: false,
      isMetricDataLoading: true,
      alertHasDimensions: isPresent(exploreDimensions)
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

    // Fetch all anomalies we have Ids for. Enhance the data and populate power-select filter options.
    // TODO: look into possibility of bundling calls or using async/await: https://github.com/linkedin/pinot/pull/2468
    if (notCreateError) {
      fetchCombinedAnomalies(anomalyIds)
        .then((rawAnomalyData) => {
          rawAnomalies = rawAnomalyData;
          return fetchSeverityScores(anomalyIds);
        })
        .then((severityScores) => {
          anomalyData = enhanceAnomalies(rawAnomalies, severityScores);
          resolutionOptions.push(...new Set(anomalyData.map(record => record.anomalyFeedback)));
          dimensionOptions.push(...new Set(anomalyData.map(anomaly => anomaly.dimensionString)));
          controller.setProperties({
            anomaliesLoaded: true,
            anomalyData,
            resolutionOptions,
            dimensionOptions
          });
          return this.fetchCombinedAnomalyChangeData(anomalyData);
        })
        // Load and display rest of options once data is loaded ('2week', 'Last Week')
        .then((wowData) => {
          anomalyData.forEach((anomaly) => {
            anomaly.wowData = wowData[anomaly.anomalyId] || {};
          });
          controller.setProperties({
            anomalyData,
            baselineOptions: [baselineOptions[0], ...newWowList]
          });
          return fetch(metricDataUrl).then(checkStatus);
        })
        // Fetch and load graph metric data. Stop spinner/loader
        .then((metricData) => {
          Object.assign(metricData, { color: metricDataColor });
          controller.setProperties({
            metricData,
            alertDimension,
            topDimensions: [],
            isMetricDataLoading: false
          });
          // If alert has dimensions set, load them into graph once replay is done.
          if (exploreDimensions && !isReplayPending) {
            controller.set('topDimensions', getTopDimensions(metricData, dimensionCount));
          }
        })
        .catch((errors) => {
          controller.setProperties({
            isMetricDataInvalid: true,
            isMetricDataLoading: false,
            graphMessageText: 'Error loading metric data'
          });
        });
    }
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  /**
   * Fetches change rate data for each available anomaly id
   * @method fetchCombinedAnomalyChangeData
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
