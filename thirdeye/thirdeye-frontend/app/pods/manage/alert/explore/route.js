/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import fetch from 'fetch';
import moment from 'moment';
import Route from '@ember/routing/route';
import { checkStatus, postProps, buildDateEod, toIso } from 'thirdeye-frontend/helpers/utils';
import { enhanceAnomalies, toIdGroups, setUpTimeRangeOptions } from 'thirdeye-frontend/helpers/manage-alert-utils';

/**
 * Shorthand for setting date defaults
 */
const dateFormat = 'YYYY-MM-DD';

/**
 * Basic alert page defaults
 */
const paginationDefault = 10;
const durationDefault = '1m';
const durationMap = { m:'month', d:'day', w:'week' };
const startDateDefault = buildDateEod(1, 'month');
const endDateDefault = buildDateEod(1, 'day');

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {Ember.RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  let anomalyPromises = [];
  if (anomalyIds.length) {
    const idGroups = toIdGroups(anomalyIds);
    const anomalyPromiseHash = idGroups.map((group, index) => {
      let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
      let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
      return Ember.RSVP.resolve(getAnomalies);
    });
    anomalyPromises = Ember.RSVP.all(anomalyPromiseHash);
  }
  return anomalyPromises;
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
    endDate: queryParamsConfig
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
    const { id, alertData, replayId } = this.modelFor('manage.alert');
    if (!id) { return; }

    const {
      duration = durationDefault,
      startDate = startDateDefault,
      endDate = endDateDefault
    } = transition.queryParams;

    // Prepare endpoints for eval, mttd, projected metrics calls
    const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const tuneUrl = `/detection-job/autotune/filter/${id}?${tuneParams}`;
    const evalUrl = `/detection-job/eval/filter/${id}?${tuneParams}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}`;
    const initialPromiseHash = {
      evalData: fetch(evalUrl).then(checkStatus),
      autotuneId: fetch(tuneUrl, postProps('')).then(checkStatus),
      mttd: fetch(mttdUrl).then(checkStatus)
    };

    return Ember.RSVP.hash(initialPromiseHash)
      .then((alertEvalMetrics) => {
        return {
          id,
          replayId,
          alertData,
          duration,
          startDate,
          endDate,
          tuneParams,
          alertEvalMetrics
        };
      })
      .catch((err) => {
        // TODO: Display default error banner in the event of fetch failure
      });
  },

  afterModel(model) {
    this._super(model);

    const {
      id: alertId,
      alertData,
      replayId,
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

    // Load endpoints for projected metrics
    const qsParams = `start=${baseStart.utc().format(dateFormat)}&end=${baseEnd.utc().format(dateFormat)}&useNotified=true`;
    const tuneParams = `start=${toIso(startDate)}&end=${toIso(endDate)}`;
    const anomalyDataUrl = `/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=`;
    const projectedUrl = `/detection-job/eval/autotune/${alertEvalMetrics.autotuneId}?${tuneParams}`;
    const projectedMttdUrl = `/detection-job/eval/projected/mttd/${alertEvalMetrics.autotuneId}`;
    const metricsUrl = `/data/autocomplete/metric?name=${dataset}::${metricName}`;
    const anomaliesUrl = `/dashboard/anomaly-function/${alertId}/anomalies?${qsParams}`;

    const promiseHash = {
      projectedMttd: fetch(projectedMttdUrl).then(checkStatus),
      projectedEval: fetch(projectedUrl).then(checkStatus),
      metricsByName: fetch(metricsUrl).then(checkStatus),
      anomalyIds: fetch(anomaliesUrl).then(checkStatus)
    };

    // Set initial value for metricId for early transition cases
    let metricId = '';

    Object.assign(model, { startStamp, endStamp, alertEvalMetrics, anomalyDataUrl, replayId });

    return Ember.RSVP.hash(promiseHash)
      .then((data) => {
        const totalAnomalies = data.anomalyIds.length;
        metricId = data.metricsByName.length ? data.metricsByName.pop().id : '';
        Object.assign(data.projectedEval, { mttd: data.projectedMttd });
        Object.assign(model.alertEvalMetrics, { projected: data.projectedEval });
        Object.assign(model, { anomalyIds: data.anomalyIds, totalAnomalies, anomalyDataUrl });
        return fetchCombinedAnomalies(data.anomalyIds);
      })

      // Fetch all anomaly data for returned Ids to paginate all from one array
      .then((rawAnomalyData) => {
        Object.assign(model, { rawAnomalyData });
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })

      // Fetch max data time for this metric (prep call for graph data) - how much data can be displayed?
      // Note: In the event of custom date selection, the end date might be less than maxTime
      .then((maxTime) => {
        const dimension = exploreDimensions || 'All';
        const currentEnd = moment(maxTime).isValid()
          ? moment(maxTime).valueOf()
          : buildDateEod(1, 'day').valueOf();
        const formattedFilters = JSON.stringify(parseProps(filters));
        const baselineStart = moment(startStamp).subtract(1, 'week').valueOf();
        const graphEnd = (endStamp < currentEnd) ? endStamp : currentEnd;
        const baselineEnd = moment(graphEnd).subtract(1, 'week');
        const metricDataUrl =  `/timeseries/compare/${metricId}/${startStamp}/${graphEnd}/` +
          `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=` +
          `${bucketSize + '_' + bucketUnit}&filters=${encodeURIComponent(formattedFilters)}&minDate=${baseEnd}&maxDate=${baseStart}`;

        Object.assign(model, { maxTime, metricDataUrl });
      })

      // Got errors?
      .catch((err) => {
        Object.assign(model, { loadError: true, loadErrorMsg: err });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      replayId,
      alertData,
      email,
      filters,
      duration,
      startStamp,
      endStamp,
      loadError,
      metricDataUrl,
      totalAnomalies,
      anomalyDataUrl,
      alertEvalMetrics,
      allConfigGroups,
      allAppNames,
      rawAnomalyData
    } = model;

    const resolutionOptions = ['All Resolutions'];
    const dimensionOptions = ['All Dimensions'];

    // Set up the response type options for anomalies
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
      { name: 'I don\'t know',
        value: 'NO_FEEDBACK',
        status: 'Not Resolved'
      },
      { name: 'Confirmed - New Trend',
        value: 'ANOMALY_NEW_TREND',
        status: 'New Trend'
      }
    ];

    // Clean array for response options power-select
    const responseOptions = anomalyResponseObj.map(response => response.name);
    const anomalyData = enhanceAnomalies(rawAnomalyData);

    // Set up options for resolution filter dropdown based on existing values
    resolutionOptions.push(...new Set(anomalyData.map(record => record.anomalyFeedback)));
    const timeRangeOptions = setUpTimeRangeOptions(['1m', '2w', '1w'], duration);

    // Prime the controller
    controller.setProperties({
      loadError,
      replayId,
      alertId: id,
      allConfigGroups,
      allAppNames,
      metricDataUrl,
      totalAnomalies,
      anomalyDataUrl,
      responseOptions,
      timeRangeOptions,
      resolutionOptions,
      alertData,
      emailData: email,
      anomalyResponseObj,
      filterData: filters,
      anomalyData,
      alertEvalMetrics,
      activeRangeStart: startStamp,
      activeRangeEnd: endStamp,
      isGraphReady: false,
      isReplayPending: Ember.isPresent(model.replayId),
      isReplayStatusError: model.replayId === 'err',
      dimensionOptions: Array.from(new Set(dimensionOptions))
    });

    controller.initialize();
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  actions: {
    // Fetch supplemental data after template has rendered (like graph)
    didTransition() {
      this.controller.fetchDeferredAnomalyData();
    }
  }
});
