/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import Ember from 'ember';
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import { checkStatus, pluralizeTime } from 'thirdeye-frontend/helpers/utils';

/**
 * Shorthand for setting date defaults
 */
const buildDate = (unit, type) => moment().subtract(unit, type).endOf('day').utc();
const dateFormat = 'YYYY-MM-DD';

/**
 * Basic alert page defaults
 */
const paginationDefault = 10;
const durationDefault = '1m';
const durationMap = { 'm':'month', 'd':'day', 'w':'week' };
const startDateDefault = buildDate(1, 'month').valueOf();
const endDateDefault = buildDate(1, 'day');

/**
 * Basic headers for any post request needed
 */
const postProps = {
  method: 'POST',
  headers: {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  }
};

/**
 * Parses stringified object from payload
 * @param {String} filters
 * @returns {Object}
 */
const parseProps = (filters) => {
  filters = filters || '';

  return filters.split(';')
    .filter(prop => prop)
    .map(prop => prop.split('='))
    .reduce(function (aggr, prop) {
      const [ propName, value ] = prop;
      aggr[propName] = value;
      return aggr;
    }, {});
};

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {Ember.RSVP promise}
 */
const fetchCombinedAnomalies = (anomalyIds) => {
  const anomalyPromises = [];

  // Split array of all ids into buckets of 10 (paginationDefault)
  const idGroups = anomalyIds.map((item, index) => {
    return (index % paginationDefault === 0) ? anomalyIds.slice(index, index + paginationDefault) : null;
  }).filter(item => item);

  // Loop on number of pages (paginationDefault) of anomaly data to fetch
  for (let i = 0; i < idGroups.length; i++) {
    let idString = encodeURIComponent(idGroups[i].toString());
    let getAnomalies = fetch(`/anomalies/search/anomalyIds/0/0/${i + 1}?anomalyIds=${idString}`).then(checkStatus);
    anomalyPromises.push(Ember.RSVP.resolve(getAnomalies));
  }

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
  const baseStart = isCustomDate ? moment(parseInt(start, 10)) : buildDate(querySize, queryUnit);
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

const replaceConfig = {
  replace: true
};

export default Ember.Route.extend({
  queryParams: {
    replay: replaceConfig,
    replayId: replaceConfig,
    duration: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },

  beforeModel(transition) {
    const target = transition.targetName;
    const { duration, startDate } = transition.queryParams;
    const id = transition.params[target].alertId;

    // Default to 1 month of anomalies to show if no dates present in query params
    if (!duration || !startDate) {
      this.transitionTo(target, id, { queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault
      }});
    }
  },

  model(params) {
    const {
      replay,
      replayId,
      duration,
      startDate,
      endDate,
      alertId: id
    } = params;
    if (!id) { return; }

    return Ember.RSVP.hash({
      id,
      replay,
      replayId,
      duration,
      startDate,
      endDate,
      alert: fetch(`/onboard/function/${id}`).then(checkStatus),
      email: fetch(`/thirdeye/email/function/${id}`).then(checkStatus)
      // TODO: enable the requests below when ready
      // alertEval: fetch(`/api/detection-job/eval/filter/${id}?start=${startDate}&end=${endDate}`).then(checkStatus),
      // projected: fetch(`/eval/projected/${id}`).then(checkStatus),
      // mttd: fetch(`/eval/mttd/${id}`).then(checkStatus)
    });
  },

  afterModel(model) {
    this._super(model);

    // Pull alert properties into context
    const {
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters,
      bucketSize,
      bucketUnit,
      id
    } = model.alert;

    // Derive start/end time ranges based on querystring input with fallback on default '1 month'
    const { startStamp, endStamp, baseStart, baseEnd } = processRangeParams(
      bucketUnit,
      model.duration,
      model.startDate,
      model.endDate
    );

    // Placeholder for incoming alert metrics
    const alertEvalMetrics = {
      eval: {},
      mttd: {},
      projected: {}
    }

    // Set initial value for metricId for early transition cases
    let metricId = '';

    // TODO: remove placeholders for alertEval/mttd/projected
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
        const currentEnd = moment(maxTime).isValid() ? moment(maxTime).valueOf() : buildDate(1, 'day').valueOf();
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
      alert,
      email,
      filters,
      replay,
      replayId,
      duration,
      startStamp,
      endStamp,
      loadError,
      metricDataUrl,
      totalAnomalies,
      anomalyDataUrl,
      anomalyMetrics,
      alertEvalMetrics,
      anomalies
    } = model;

    const resolutionOptions = ['All Resolutions'];
    const dimensionOptions = ['All Dimensions'];

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
        start: buildDate(2, 'week').valueOf(),
        isActive: false
      },
      {
        name: 'Last Week',
        value: '1w',
        start: buildDate(1, 'week').valueOf(),
        isActive: false
      },
      {
        name: 'Custom',
        value: 'custom',
        start: null,
        isActive: false
      }
    ];

    // Do we have a matching querystring param for our range options?
    const responseOptions = anomalyResponseObj.map(response => response.name);
    const matchingDuration = timeRangeOptions.find((range) => range.value === duration);

    // NOTE: these will be loaded from a new endpoint
    const alertStats = anomalyMetrics ? {
      response: Ember.getWithDefault(anomalyMetrics, 'totalResponses', 0) * 100 / totalAnomalies,
      presicion: anomalyMetrics.precision * 100 || 0,
      recall: anomalyMetrics.recall * 100 || 0,
      mttd: '4.8 mins'
    } : {};

    // Select which duration bucket we are in
    if (matchingDuration) {
      matchingDuration.isActive = true;
    } else {
      timeRangeOptions.find((range) => range.name === 'Custom').isActive = true;
    }

    // Loop over all anomalies to configure display settings
    for (var anomaly of anomalies) {
      const anomalyDuration = moment.duration(moment(anomaly.anomalyEnd).diff(moment(anomaly.anomalyStart)));
      const dimensionList = [];
      const curr = anomaly.current;
      const base = anomaly.baseline;
      const days = anomalyDuration.get("days");
      const hours = anomalyDuration.get("hours");
      const minutes = anomalyDuration.get("minutes");
      const durationArr = [pluralizeTime(days, 'day'), pluralizeTime(hours, 'hour'), pluralizeTime(minutes, 'minute')];

      // Placeholder: ChangeRate will not be calculated on front-end
      const changeRate = (curr && base) ? ((curr - base) / base * 100).toFixed(2) : 0;

      // We want to display only non-zero duration values
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
          let dimensionVal = Object.values(dimensionObj).join(',').toUpperCase();
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
      alertStats,
      metricDataUrl,
      totalAnomalies,
      anomalyDataUrl,
      responseOptions,
      timeRangeOptions,
      resolutionOptions,
      dimensionOptions,
      alertData: alert,
      emailData: email,
      anomalyResponseObj,
      filterData: filters,
      anomalyData: anomalies,
      evalData: alertEvalMetrics,
      activeRangeStart: startStamp,
      activeRangeEnd: endStamp,
      isGraphReady: false
    });

    controller.initialize(replay);
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
