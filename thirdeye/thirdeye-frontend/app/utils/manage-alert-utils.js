import Ember from 'ember';
import _ from 'lodash';
import moment from 'moment';
import { buildDateEod } from 'thirdeye-frontend/utils/utils';

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
  return isFinite(metric) ? formattedNum : shown;
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
    const startMoment = moment(anomaly.anomalyStart);
    const endMoment = moment(anomaly.anomalyEnd);
    const anomalyDuration = moment.duration(endMoment.diff(startMoment));
    const days = anomalyDuration.get("days");
    const hours = anomalyDuration.get("hours");
    const minutes = anomalyDuration.get("minutes");
    const score = resolvedScores.length ? resolvedScores.find(score => score.id === anomaly.anomalyId).score : null;
    const durationArr = [pluralizeTime(days, 'day'), pluralizeTime(hours, 'hour'), pluralizeTime(minutes, 'minute')];

    // Set up anomaly change rate display
    const changeRate = (anomaly.current && anomaly.baseline) ? ((anomaly.current - anomaly.baseline) / anomaly.baseline * 100).toFixed(2) : 0;
    const changeDirection = (anomaly.current > anomaly.baseline) ? '-' : '+';
    const changeDirectionLabel = changeRate < 0 ? 'down' : 'up';

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
      changeDirection,
      changeDirectionLabel,
      shownChangeRate: changeRate,
      isUserReported: anomaly.anomalyResultSource === 'USER_LABELED_ANOMALY',
      startDateStr: moment(anomaly.anomalyStart).format('MMM D, hh:mm A'),
      durationStr: noZeroDurationArr.join(', '),
      severityScore: score ? score.toFixed(2) : 'N/A',
      shownCurrent: anomaly.current,
      shownBaseline: anomaly.baseline,
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

  const dateKeyMap = new Map([
    [ '1m', ['Last 30 Days', 1, 'month'] ],
    [ '3m', ['3 Months', 3, 'month'] ],
    [ '2w', ['Last 2 Weeks', 2, 'week'] ],
    [ '1w', ['Last Week', 1, 'week'] ]
  ]);

  datesKeys.forEach((value) => {
    let currVal = dateKeyMap.get(value);
    let name = currVal[0];
    let start = moment().subtract(currVal[1], currVal[2]).endOf('day').utc();
    let isActive = duration === value;
    newRangeArr.push({ name, value, start, isActive });
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
  const { id, maxTime, filters, dimension, granularity } = graphConfig;
  // Chosen dimension
  const selectedDimension = dimension || 'All';
  // Do not send a filters param if value not present
  const filterQs = filters ? `&filters=${encodeURIComponent(filters)}` : '';
  // Load only a week of data if granularity is high
  const startTimeBucket = granularity && granularity.toLowerCase().includes('minute') ? 'week' : 'months';
  // For end date, choose either maxTime or end of yesterday
  const currentEnd = moment(maxTime).isValid() ? moment(maxTime).valueOf() : buildDateEod(1, 'day').valueOf();
  // For graph start date, take either 1 week or 1 month, depending on granularity
  const currentStart = moment(currentEnd).subtract(1, startTimeBucket).valueOf();
  // Baseline starts 1 week before our start date
  const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
  // Baseline ends 1 week before our end date
  const baselineEnd = moment(currentEnd).subtract(1, 'week');
  // Now build the metric data url
  return `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=` +
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
    let average = changeArr.reduce((previous, current) => current += previous) / changeArr.length;
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
    if (newData) {
      stat.projected = isTotal ? newData : formatEvalMetric(newData, isPercentageMetric);
      stat.projectedUnits = isFinite(newData) ? stat.units : null;
      stat.showDirectionIcon = isFinite(origData) && isFinite(newData) && origData !== newData;
      stat.direction = stat.showDirectionIcon && origData > newData ? 'bottom' : 'top';
    }
  });

  return anomalyStats;
}

/**
 * When fetching current and projected MTTD (minimum time to detect) data, we need to supply the
 * endpoint with a severity threshold. This decides whether to use the default or not.
 * @method extractSeverity
 * @param {Number} defaultSeverity - number to fall back on if we have none defined in alert filter
 * @return {undefined}
 */
export function extractSeverity(alertData, defaultSeverity) {
  const alertFilterSeverity = Ember.getWithDefault(alertData, 'alertFilter.mttd', null);
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
  buildAnomalyStats,
  buildMetricDataUrl,
  extractSeverity,
  evalObj
};
