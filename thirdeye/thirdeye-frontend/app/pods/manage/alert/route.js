import Ember from 'ember';
import fetch from 'fetch';
import moment from 'moment';
import { checkStatus, checkStatusPost } from 'thirdeye-frontend/helpers/utils';

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
 * Ensures requested duration does not exceed 1 month
 * @param {Number} time - quantity of time for analysis
 * @param {String} unit - 'day' or 'month'
 * @returns {Object}
 */
const calculatedDuration = (time, unit) => {
  if (time > 30) {
    time = 30;
  }
  if (unit === 'month') {
    time = 1;
  }
  return { time, unit };
};

/**
 * Fetches all anomaly data, up to 2000 (querystring length limit on GET)
 * To take pagination from server, use call below. Unfortunately, server pagination does not support dimension filtering
 * fetch(`/anomalies/search/anomalyIds/${startStamp}/${endStamp}/1?anomalyIds=${idString}`).then(checkStatus)
 * @param {String} idStr
 * @param {Number} total
 * @param {Number} start
 * @param {Number} end
 * @returns {Ember.RSVP promise}
 */
const fetchCombinedAnomalies = (idStr, total, start, end, limit = 2000) => {
  const anomalyPromises = [];
  total = (total > limit) ? limit : total;
  for (let index = 1; index < total/10 + 1; index++) {
    let getAnomalies = fetch(`/anomalies/search/anomalyIds/${start}/${end}/${index}?anomalyIds=${idStr}`).then(checkStatus);
    anomalyPromises.push(Ember.RSVP.resolve(getAnomalies));
  }
  return Ember.RSVP.all(anomalyPromises);
};

/**
 * Constructs the currently loaded anomaly analysis window
 * @param {Object} durationObj - example { time: '10', unit: 'days' }
 * @param {Date} baseDate - the base start date
 * @returns {Object}
 */
const durationString = (durationObj, baseDate) => {
  const startDate = baseDate.utc().format("DD-MM");
  if (durationObj.time > 1) {
    durationObj.unit = durationObj.unit + 's';
  }
  return `Last ${durationObj.time} ${durationObj.unit} (${startDate} to Today)`;
};

/**
 * Setup for query param behavior
 */
const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

const replaceConfig = {
  replace: true
};

export default Ember.Route.extend({
  queryParams: {
    replay: replaceConfig,
    replayId: replaceConfig,
    duration: queryParamsConfig
  },

  model(params) {
    const {
      replay,
      replayId,
      duration,
      alertId: id
    } = params;
    if (!id) { return; }

    return Ember.RSVP.hash({
      id,
      replay,
      replayId,
      duration,
      alert: fetch(`/onboard/function/${id}`).then(checkStatus),
      email: fetch(`/thirdeye/email/function/${id}`).then(checkStatus)
    });
  },

  afterModel(model) {
    this._super(model);

    const {
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      functionName,
      filters,
      bucketSize,
      bucketUnit,
      id
     } = model.alert;

    // Form start/end time ranges based on querystring input with fallback on default '1 month'
    const durationMap = { 'm':'month', 'd':'day' };
    const durationMatch = model.duration ? model.duration.match(new RegExp(/^(.\d)+([d|m])$/i)) : [];
    const queryUnit = durationMatch.length ? durationMap[durationMatch[1].toLowerCase()] : 'month';
    const durationWindow = calculatedDuration(durationMatch[1] || 1, queryUnit);
    const baseStart = moment().subtract(durationWindow.time, durationWindow.unit).endOf('day');
    const baseEnd = moment().subtract(1, 'day').endOf('day');
    const startDate = baseStart.utc().format("YYYY-MM-DD");
    const endDate = baseEnd.utc().format("YYYY-MM-DD");
    const startStamp = baseStart.valueOf();
    const endStamp = baseEnd.valueOf();

    const postProps = {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      }
    };

    let metricId = '';
    let allGroupNames = [];
    let allGroups = [];
    let metricDataUrl = '';
    let metricDimensionURl = '';
    let selectedAppName = '';

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      .then((metricsByName) => {
        const metric = metricsByName.pop();
        metricId = metric.id;
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })
      .then((maxTime) => {
        const dimension = exploreDimensions || 'All';
        const currentEnd = moment(maxTime).isValid()
          ? moment(maxTime).valueOf()
          : moment().subtract(1, 'day').endOf('day').valueOf();
        const formattedFilters = JSON.stringify(parseProps(filters));

        // Load less data if granularity is 'minutes'
        const isMinutely = bucketUnit.toLowerCase().includes('minute');
        const duration = isMinutely ? { unit: 2, size: 'week' } : { unit: 1, size: 'month' };
        const currentStart = moment(currentEnd).subtract(duration.unit, duration.size).valueOf();
        const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
        const baselineEnd = moment(currentEnd).subtract(1, 'week');

        // Prepare call for metric graph data
        metricDataUrl =  `/timeseries/compare/${metricId}/${currentStart}/${currentEnd}/` +
          `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=` +
          `${bucketSize + '_' + bucketUnit}&filters=${encodeURIComponent(formattedFilters)}`;

        // Prepare call for dimension graph data
        metricDimensionURl = `/rootcause/query?framework=relatedDimensions&anomalyStart=${currentStart}` +
          `&anomalyEnd=${currentEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}` +
          `&analysisStart=${currentStart}&analysisEnd=${currentEnd}&urns=thirdeye:metric:${metricId}` +
          `&filters=${encodeURIComponent(filters)}`;

        // Fetch graph metric data
        return fetch(metricDataUrl).then(checkStatus);
      })
      .then((metricData) => {
        Object.assign(metricData, { color: 'blue' })
        Object.assign(model, { metricData });
        return fetch(`/dashboard/anomaly-function/${model.id}/anomalies?start=${startDate}&end=${endDate}`).then(checkStatus);
      })
      // Fetch all anomaly Ids for current alert
      .then((anomalyIds) => {
        const totalAnomalies = anomalyIds.length;
        const idString = encodeURIComponent(anomalyIds.toString().substring(0, 2000));
        if (Ember.isEmpty(anomalyIds)) { return []; }
        else {
          Object.assign(model, { totalAnomalies });
          return fetchCombinedAnomalies(idString, totalAnomalies, startStamp, endStamp);
        }
      })
      // Fetch all anomaly data for returned Ids
      .then((anomalyData) => {
        const anomalies = [];
        // Concatenate all available anomalies into one array to paginate on front-end
        // TODO: make sure count is accurate
        anomalyData.forEach(data => {
          anomalies.push(...data.anomalyDetailsList);
        });
        // These props are the same for each record, so take it from the first one
        const filterMaps = anomalyData[0] ? anomalyData[0].searchFilters || [] : [];
        const anomalyCount = anomalyData[0] ? anomalyData[0].totalAnomalies : 0;
        Object.assign(model, { anomalies, filterMaps, anomalyCount, baseStart, durationWindow });
        const metricsUrl = `/detection-job/eval/filter/${model.id}?start=${startDate}&end=${endDate}`;
        return fetch(metricsUrl, postProps).then((res) => checkStatus(res, 'post'));
      })
      // Fetch anomaly metrics for this alert
      .then((metrics) => {
        Object.assign(model, { anomalyMetrics: metrics });
      })
      .catch((errors) => {
        Object.assign(model, { loadError: true, loadErrorMsg: errors });
      });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const selectedRange = ''
    const dimensionOptions = ['All Dimensions'];
    const {
      id,
      alert,
      email,
      filters,
      replay,
      replayId,
      baseStart,
      metricData,
      totalAnomalies,
      durationWindow,
      anomalyMetrics,
      anomalies
    } = model;

    // Loop over all anomalies to configure display settings
    anomalies.forEach((anomaly) => {
      const dimensionList = [];
      let curr = anomaly.current;
      let base = anomaly.baseline;
      // Set 'not reviewed' label
      if (!anomaly.anomalyFeedback) {
        anomaly.anomalyFeedback = 'Not reviewed yet';
      }
      // Set change rate for table
      anomaly.changeRate = (curr && base) ? ((curr - base) / base * 100).toFixed(2) : 0;
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
    });

    controller.setProperties({
      metricData,
      alertId: id,
      totalAnomalies,
      durationWindow,
      dimensionOptions,
      alertData: alert,
      emailData: email,
      filterData: filters,
      anomalyData: anomalies,
      rangeOptions: [durationString(model.durationWindow, model.baseStart)],
      selectedRangeOption: durationString(model.durationWindow, model.baseStart)
    });

    controller.initialize(replay);
  },

  resetController(controller, isExiting, transition) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

});
