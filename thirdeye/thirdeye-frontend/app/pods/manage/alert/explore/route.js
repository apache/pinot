/**
 * Handles the 'explore' route for manage alert
 * @module manage/alert/edit/explore
 * @exports manage/alert/edit/explore
 */
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import Route from '@ember/routing/route';
import { checkStatus, pluralizeTime, buildDateEod, parseProps } from 'thirdeye-frontend/helpers/utils';

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
  // Split array of all ids into buckets of 10 (paginationDefault)
  const idGroups = anomalyIds.map((item, index) => {
    return (index % paginationDefault === 0) ? anomalyIds.slice(index, index + paginationDefault) : null;
  }).filter(item => item);

  // Loop on number of pages (paginationDefault) of anomaly data to fetch
  const anomalyPromises = idGroups.map((group, index) => {
    let idStringParams = `anomalyIds=${encodeURIComponent(idGroups[index].toString())}`;
    let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${index + 1}?${idStringParams}`).then(checkStatus);
    return Ember.RSVP.resolve(getAnomalies);
  });

  return Ember.RSVP.all(anomalyPromises);
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
    const { id, alertData } = this.modelFor('manage.alert');
    if (!id) { return; }

    const {
      duration = durationDefault,
      startDate = startDateDefault,
      endDate = endDateDefault
    } = transition.queryParams;

    // Prepare endpoints for eval, mttd, projected metrics calls
    const isoStart = moment(Number(startDate)).toISOString();
    const isoEnd = moment(Number(endDate)).toISOString();
    const tuneParams = `start=${isoStart}&end=${isoEnd}&autotunetype=AUTOTUNE`;
    const autoTuneUrl = `/detection-job/autotune/filter/${id}?${tuneParams}`;
    const evalUrl = `/detection-job/eval/projected/${id}`;
    const mttdUrl = `/detection-job/eval/mttd/${id}`;

    // TODO: apply calls for alertEvalMetrics from go/te-ss-alert-flow-api wiki
    const promiseHash = {
      eval: null,
      projected: null,
      mttd: fetch(mttdUrl).then(checkStatus)
    };

    return Ember.RSVP.hash(promiseHash)
      .then((alertEvalMetrics) => {
        return {
          id,
          alertData,
          duration,
          startDate,
          endDate,
          alertEvalMetrics
        };
      });
  },

  afterModel(model) {
    this._super(model);

    const {
      id,
      alertData,
      duration,
      startDate,
      endDate,
      autoTune,
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
    } = processRangeParams( bucketUnit, duration, startDate, endDate );

    // Set initial value for metricId for early transition cases
    let metricId = '';

    Object.assign(model, { startStamp, endStamp, alertEvalMetrics });
    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      // Fetch the metric Id for the current alert
      .then((metricsByName) => {
        metricId = metricsByName.length ? metricsByName.pop().id : '';
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })

      // Fetch max data time for this metric (prep call for graph data) - how much data can be displayed?
      // Note: In the event of custom date selection, the end date might be less than maxTime
      .then((maxTime) => {
        const dimension = exploreDimensions || 'All';
        const startDate = baseStart.utc().format(dateFormat);
        const endDate = baseEnd.utc().format(dateFormat);
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
        return fetch(`/dashboard/anomaly-function/${id}/anomalies?start=${startDate}&end=${endDate}`).then(checkStatus);
      })

      // Fetching first page of anomaly Ids for current alert
      .then((anomalyIds) => {
        const totalAnomalies = anomalyIds.length;
        const anomalyDataUrl = `/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=`;
        if (Ember.isEmpty(anomalyIds)) { return []; }
        else {
          Object.assign(model, { anomalyIds, totalAnomalies, anomalyDataUrl });
          return fetchCombinedAnomalies(anomalyIds, true);
        }
      })

      // Fetch all anomaly data for returned Ids to paginate all from one array
      // TODO: load only first page, and defer loading the rest
      .then((rawAnomalyData) => {
        //rawAnomalyData.forEach(data => anomalies.push(...data.anomalyDetailsList));
        const anomalies = [].concat(...rawAnomalyData.map(data => data.anomalyDetailsList));
        // These props are the same for each record, so take it from the first one
        const filterMaps = rawAnomalyData[0] ? rawAnomalyData[0].searchFilters || [] : [];
        const anomalyCount = rawAnomalyData[0] ? rawAnomalyData[0].totalAnomalies : 0;
        Object.assign(model, { anomalies, filterMaps, anomalyCount });
      })

      // Got errors?
      .catch((errors) => {
        Object.assign(model, { loadError: true, loadErrorMsg: errors });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
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
      anomalies
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

    // Set up time range options for entire page
    const timeRangeOptions = [
      {
        name: 'Last 30 Days',
        value: '1m',
        start: startDateDefault,
        isActive: false
      },
      {
        name: 'Last 2 Weeks',
        value: '2w',
        start: buildDateEod(2, 'week').valueOf(),
        isActive: false
      },
      {
        name: 'Last Week',
        value: '1w',
        start: buildDateEod(1, 'week').valueOf(),
        isActive: false
      },
      {
        name: 'Custom',
        value: 'custom',
        start: null,
        isActive: false
      }
    ];

    // Does the duration query param match one of our range options? (for highlighting)
    const matchingDuration = timeRangeOptions.find((range) => range.value === duration);
    if (matchingDuration) {
      matchingDuration.isActive = true;
    } else {
      timeRangeOptions.find((range) => range.name === 'Custom').isActive = true;
    }

    // Loop over all anomalies to configure display settings
    for (var anomaly of anomalies) {
      let dimensionList = [];
      const startMoment = moment(anomaly.anomalyStart);
      const endMoment = moment(anomaly.anomalyEnd);
      const anomalyDuration = moment.duration(endMoment.diff(startMoment));
      const days = anomalyDuration.get("days");
      const hours = anomalyDuration.get("hours");
      const minutes = anomalyDuration.get("minutes");
      const durationArr = [pluralizeTime(days, 'day'), pluralizeTime(hours, 'hour'), pluralizeTime(minutes, 'minute')];

      // Placeholder: ChangeRate will not be calculated on front-end
      const changeRate = (anomaly.current && anomaly.baseline)
        ? ((anomaly.current - anomaly.baseline) / anomaly.baseline * 100).toFixed(2) : 0;

      // We want to display only non-zero duration values in our table
      const noZeroDurationArr = _.remove(durationArr, function(item) {
        return Ember.isPresent(item);
      });

      // Set 'not reviewed' label
      if (!anomaly.anomalyFeedback) {
        anomaly.anomalyFeedback = 'Not reviewed yet';
      }

      // Add missing properties
      Object.assign(anomaly, {
        changeRate,
        shownChangeRate: changeRate,
        startDateStr: moment(anomaly.anomalyStart).format('MMM D, hh:mm A'),
        durationStr: noZeroDurationArr.join(', '),
        severityScore: (anomaly.current/anomaly.baseline - 1).toFixed(2),
        shownCurrent: anomaly.current,
        shownBaseline: anomaly.baseline,
        showResponseSaved: false,
        shorResponseFailed: false
      });

      // Create a list of all available dimensions for toggling. Also massage dimension property.
      if (anomaly.anomalyFunctionDimension) {
        let dimensionObj = JSON.parse(anomaly.anomalyFunctionDimension);
        for (let dimension of Object.keys(dimensionObj)) {
          let dimensionKey = dimension.dasherize();
          let dimensionVal = dimensionObj[dimension].join(',');
          dimensionList.push({ dimensionKey, dimensionVal });
          dimensionOptions.push(`${dimensionKey}:${dimensionVal}`);
        }
        Object.assign(anomaly, { dimensionList });
      }
    }

    // Set up options for resolution filter dropdown based on existing values
    resolutionOptions.push(...new Set(anomalies.map(record => record.anomalyFeedback)));

    // Prime the controller
    controller.setProperties({
      loadError,
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
      anomalyData: anomalies,
      alertEvalMetrics,
      activeRangeStart: startStamp,
      activeRangeEnd: endStamp,
      isGraphReady: false,
      dimensionOptions: Array.from(new Set(dimensionOptions))
    });

    controller.initialize(false);
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
