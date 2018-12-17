import moment from 'moment';
import { isPresent, isBlank } from "@ember/utils";
import { getWithDefault } from '@ember/object';
import { buildDateEod } from 'thirdeye-frontend/utils/utils';
import { getFormatedDuration } from 'thirdeye-frontend/utils/anomaly';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';

/**
 * Handles types and defaults returned from eval/projected endpoints
 * @param {Number|String} metric - number or string like 'NaN', 'Infinity'
 * @param {Boolean} isPercentage - shall we treat this as a % or a whole number?
 * @returns {Object}
 */
export function formatEvalMetric(metric, isPercentage = false) {
  const isWhole = Number.isInteger(Number(metric));
  let shown = (metric === 'Infinity') ? metric : 'N/A';
  const multiplier = isPercentage ? 100 : 1;
  const convertedNum = metric * multiplier;
  const formattedNum = isWhole ? convertedNum : convertedNum.toFixed(1);
  let displayNum = isFinite(metric) ? formattedNum : shown;
  // Prevent meaninglessly large numbers
  return (Number(displayNum) > 10000) ? 'N/A' : displayNum;
}

/**
 * Pluralizes and formats the anomaly range duration string
 * @param {Number} time
 * @param {String} unit
 * @returns {String}
 */
export function pluralizeTime(time, unit) {
  const unitStr = time > 1 ? unit + 's' : unit;
  return time ? time + ' ' + unitStr : '';
}

/**
 * Performs a case-insensitive sort of a flat array or array of objects
 * by property. If sorting flat array, pass "null" for targetProperty.
 * @param {Array} targetArray
 * @param {String} targetProperty
 * @returns {Array}
 */
export function powerSort(targetArray, targetProperty) {
  const cleanArray = [];
  // Make sure we have a valid array
  targetArray.forEach((item) => {
    cleanArray.push(isBlank(item) ? 'undefined' : item);
  });
  // Do case-insensitive sort
  const sortedArray = cleanArray.sort((a, b) => {
    if (targetProperty) {
      a = a[targetProperty];
      b = b[targetProperty];
    }
    return a.toLowerCase().trim().localeCompare(b.toLowerCase().trim());
  });
  return sortedArray;
}

/**
 * Split array of all anomalyIds into buckets of bucketSize
 * @param {Array} anomalyIds - array of anomaly Ids
 * @param {Number} bucketSize - number of anomalies per group
 * @returns {Array}
 */
export function toIdGroups(anomalyIds, bucketSize = 10) {
  const idGroups = anomalyIds.map((item, index) => {
    return (index % bucketSize === 0) ? anomalyIds.slice(index, index + bucketSize) : null;
  }).filter(item => item);
  return idGroups;
}

/**
 * Derives and formats extra anomaly properties such as duration
 * @param {Array} anomalies - array of raw anomalies
 * @returns {Array}
 */
export function enhanceAnomalies(rawAnomalies, severityScores) {
  const newAnomalies = [];
  const anomaliesPresent = rawAnomalies && rawAnomalies.length;
  // De-dupe raw anomalies, extract only the good stuff (anomalyDetailsList)
  const anomalies = anomaliesPresent ? [].concat(...rawAnomalies.map(data => data.anomalyDetailsList)) : [];
  // Extract all resolved scores from the RSVP promise response
  const resolvedScores = severityScores ? severityScores.map((score) => {
    return (score.state === 'fulfilled') ? score.value : '';
  }) : [];

  // Loop over all anomalies to configure display settings
  anomalies.forEach((anomaly) => {
    let dimensionList = [];
    let targetAnomaly = resolvedScores.find(score => Number(score.id) === Number(anomaly.anomalyId));
    // Extract current anomaly's score from array of all scores
    const score = resolvedScores.length && targetAnomaly ? targetAnomaly.score : null;
    // Set up anomaly change rate display
    const changeRate = (anomaly.current && anomaly.baseline) ? floatToPercent((anomaly.current - anomaly.baseline) / anomaly.baseline) : 0;
    const isNullChangeRate = Number.isNaN(Number(changeRate));
    // Set 'not reviewed' label
    if (!anomaly.anomalyFeedback) {
      anomaly.anomalyFeedback = 'Not Resolved';
    }
    // Add missing properties
    Object.assign(anomaly, {
      changeRate,
      isNullChangeRate,
      shownChangeRate: changeRate,
      isUserReported: anomaly.anomalyResultSource === 'USER_LABELED_ANOMALY',
      startDateStr: moment(anomaly.anomalyStart).format('MMM D, hh:mm A'),
      durationStr: getFormatedDuration(anomaly.anomalyStart, anomaly.anomalyEnd),
      severityScore: score && !isNaN(score) ? score.toFixed(2) : 'N/A',
      shownCurrent: Number(anomaly.current) > 0 ? anomaly.current : 'N/A',
      shownBaseline: Number(anomaly.baseline) > 0 ? anomaly.baseline : 'N/A',
      showResponseSaved: false,
      showResponseFailed: false
    });
    // Create a list of all available dimensions for toggling. Also massage dimension property.
    if (anomaly.anomalyFunctionDimension) {
      let dimensionObj = JSON.parse(anomaly.anomalyFunctionDimension);
      let dimensionStrArr = [];
      for (let dimension of Object.keys(dimensionObj)) {
        let dimensionKey = dimension.dasherize();
        let dimensionVal = dimensionObj[dimension].join(',');
        dimensionList.push({ dimensionKey, dimensionVal });
        dimensionStrArr.push(`${dimensionKey}:${dimensionVal}`);
      }
      let dimensionString = dimensionStrArr.join(' & ');
      Object.assign(anomaly, { dimensionList, dimensionString });
    }
    newAnomalies.push(anomaly);
  });
  // List most recent anomalies first
  return newAnomalies.sortBy('anomalyStart').reverse();
}

/**
 * Generates time range options for selection in the self-serve UI
 * @example output
 * [{ name: "3 Months", value: "3m", start: Moment, isActive: true },
 *  { name: "Custom", value: "custom", start: null, isActive: false }]
 * @method setUpTimeRangeOptions
 * @param {Array} datesKeys - array of keys used to generate time ranges
 * @param {String} duration - the selected time span that is default
 * @return {Array}
 */
export function setUpTimeRangeOptions(datesKeys, duration) {
  const newRangeArr = [];

  const defaultCustomRange = {
    name: 'Custom',
    value: 'custom',
    start: null,
    isActive: !datesKeys.includes(duration)
  };

  const dateKeyMap = new Map(
    [
      [ '1m', ['Last 30 Days', 1, 'month'] ],
      [ '3m', ['3 Months', 3, 'month'] ],
      [ '2w', ['Last 2 Weeks', 2, 'week'] ],
      [ '1w', ['Last Week', 1, 'week'] ],
      [ '2d', ['Yesterday', 2, 'day'] ],
      [ '1d', ['Last 24 hours', 1, 'day'] ],
      [ 'today', ['Today'] ]
    ]);

   datesKeys.forEach((value) => {
     const currVal = dateKeyMap.get(value);
     const label = currVal[0];
     let start = moment().subtract(currVal[1], currVal[2]).utc();
     switch(label) {
      case 'Today':
        start = moment().startOf('day');
        break;
      case 'Yesterday':
        start = moment().subtract(1, 'day').startOf('day');
        break;
     }
     const end = (label === 'Yesterday') ? moment().subtract(1, 'days').endOf('day') : moment();
     const isActive = duration === value;
     newRangeArr.push({ name: label, value, start, end, isActive });
   });

  newRangeArr.push(defaultCustomRange);
  return newRangeArr;
}

/**
 * Returns a sample JSON anomaly eval object
 * @returns {Object}
 */
export function evalObj() {
  return {
    userReportAnomaly: 0,
    totalResponses: 0,
    trueAnomalies: 0,
    recall: NaN,
    totalAlerts: 6,
    responseRate: 0.0,
    precision: 0.0,
    falseAlarm: 0,
    newTrend: 0,
    weightedPrecision: 0.0
  };
}

/**
 * Builds the request parameters for the metric data API call.
 * TODO: Document this inline for clarity.
 * @method buildMetricDataUrl
 * @param {Object} graphConfig - the metric settings
 * @returns {String} metric data call params/url
 */
export function buildMetricDataUrl(graphConfig) {
  const { id, maxTime, startStamp, endStamp, filters, dimension, granularity } = graphConfig;
  // Chosen dimension
  const selectedDimension = dimension || 'All';
  // Do not send a filters param if value not present
  const filterQs = filters ? `&filters=${encodeURIComponent(filters)}` : '';
  // Load only a week of data in default if granularity is high
  const startTimeBucket = granularity && granularity.toLowerCase().includes('minute') ? 'week' : 'months';
  // set maxData as maxTime or default
  const maxData = maxTime && moment(maxTime).isValid() ? moment(maxTime).valueOf() : buildDateEod(1, 'day').valueOf();
  // For end date, use end stamp if defined and valid, otherwise use maxData
  const currentEnd = endStamp && moment(endStamp).isValid() ? moment(endStamp).valueOf() : moment(maxData).valueOf();
  // For graph start date, use start stamp if defined and valid, otherwise pick it usimng startTimeBucket depending on granularity
  const currentStart = startStamp && moment(startStamp).isValid() ? moment(startStamp).valueOf() : moment(currentEnd).subtract(1, startTimeBucket).valueOf();
  // Now build the metric data url -> currentEnd and currentStart reused in the call since baseline no longer displayed on graph
  return `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${currentStart}/${currentEnd}?dimension=` +
         `${selectedDimension}&granularity=${granularity}${filterQs}`;
}

/**
 * If a dimension has been selected, the metric data object will contain subdimensions.
 * This method averages each subdimension's total change rate and returns a sorted list
 * of the top X graph-ready dimension objects
 * @method getTopDimensions
 * @param {Object} metricData - the graphable metric data returned from fetchAnomalyGraphData()
 * @param {Number} dimCount - number of dimensions to allow in response
 * @return {undefined}
 */
export function getTopDimensions(metricData, dimCount) {
  const colors = ['orange', 'teal', 'purple', 'red', 'green', 'pink'];
  const dimensionObj = metricData.subDimensionContributionMap || {};
  const dimensionKeys = Object.keys(dimensionObj);
  let processedDimensions = [];
  let dimensionList = [];
  let colorIndex = 0;

  // Build the array of subdimension objects for the selected dimension
  dimensionKeys.forEach((subDimension) => {
    let subdObj = dimensionObj[subDimension];
    let changeArr = subdObj.cumulativePercentageChange.map(item => Math.abs(item));
    let average = changeArr.length ? changeArr.reduce((previous, current) => current += previous) / changeArr.length : 0;
    if (subDimension.toLowerCase() !== 'all') {
      dimensionList.push({
        average,
        name: subDimension,
        baselineValues: subdObj.baselineValues,
        currentValues: subdObj.currentValues,
        isSelected: true
      });
    }
  });
  processedDimensions = dimensionList.sortBy('average').reverse().slice(0, dimCount);
  processedDimensions.forEach((dimension) => {
    dimension.color = colors[colorIndex];
    colorIndex = colorIndex > 5 ? 0 : colorIndex + 1;
  });

  // Return the top X sorted by level of change contribution
  return processedDimensions;
}

/**
 * Data needed to render the stats 'cards' above the anomaly graph for a given alert
 * @param {Object} alertEvalMetrics - contains the alert's performance data
 * @param {Object} anomalyStats - collection of metric block definitions
 * example: {
 *    title: 'Recall',
      key: 'recall',
      units: '%',
      tooltip,
      text: 'Number of anomalies detected by the system.'
    }
 * @param {String} severity - the severity threshold entered
 * @param {Boolean} isPercent - suffix associated with the selected severity mode
 * @returns {Array}
 */
export function buildAnomalyStats(alertEvalMetrics, anomalyStats, showProjected = true) {
  anomalyStats.forEach((stat) => {
    let origData = alertEvalMetrics.current[stat.key];
    let newData = alertEvalMetrics.projected ? alertEvalMetrics.projected[stat.key] : null;
    let isPercentageMetric = stat.units === '%';
    let isTotal = stat.key === 'totalAlerts';
    stat.showProjected = showProjected;
    stat.value = isTotal ? origData : formatEvalMetric(origData, isPercentageMetric);
    stat.valueUnits = isFinite(origData) ? stat.units : null;
    if (isPresent(newData)) {
      stat.projected = isTotal ? newData : formatEvalMetric(newData, isPercentageMetric);
      stat.projectedUnits = isFinite(newData) ? stat.units : null;
      stat.showDirectionIcon = isFinite(origData) && isFinite(newData) && origData !== newData;
      stat.direction = stat.showDirectionIcon && origData > newData ? 'bottom' : 'top';
    }
  });

  return anomalyStats;
}

/**
 * Returns selected alert object properties for config group table display
 * @param {Object} alertData - a single alert record
 * @param {Number} alertIndex - record index
 * @returns {Array}
 */
export function formatConfigGroupProps(alertData, alertIndex) {
  return {
    number: alertIndex + 1,
    id: alertData.id,
    name: alertData.functionName,
    metric: alertData.metric + '::' + alertData.collection,
    owner: alertData.createdBy || 'N/A',
    status: alertData.isActive ? 'active' : 'inactive',
    isNewId: alertData.id === 0
  };
}

/**
 * When fetching current and projected MTTD (minimum time to detect) data, we need to supply the
 * endpoint with a severity threshold. This decides whether to use the default or not.
 * @method extractSeverity
 * @param {Number} defaultSeverity - number to fall back on if we have none defined in alert filter
 * @return {undefined}
 */
export function extractSeverity(alertData, defaultSeverity) {
  const alertFilterSeverity = getWithDefault(alertData, 'alertFilter.mttd', null);
  const parsedSeverity = alertFilterSeverity ? alertFilterSeverity.split(';')[1].split('=') : null;
  const isSeverityNumeric = parsedSeverity && !isNaN(parsedSeverity[1]);
  const finalSeverity = isSeverityNumeric ? parsedSeverity[1] : defaultSeverity;
  return finalSeverity;
}

export default {
  formatEvalMetric,
  toIdGroups,
  pluralizeTime,
  enhanceAnomalies,
  getTopDimensions,
  setUpTimeRangeOptions,
  formatConfigGroupProps,
  buildAnomalyStats,
  buildMetricDataUrl,
  extractSeverity,
  powerSort,
  evalObj
};
