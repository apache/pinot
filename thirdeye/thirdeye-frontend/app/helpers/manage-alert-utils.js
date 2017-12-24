import { helper } from '@ember/component/helper';
import moment from 'moment';
import fetch from 'fetch';

/**
 * Handles types and defaults returned from eval/projected endpoints
 * @param {Number|String} metric - number or string like 'NaN', 'Infinity'
 * @param {Boolean} allowDecimal - should this metric be shown as whole number?
 * @returns {Object}
 */
export function formatEvalMetric(metric, allowDecimal = false) {
  let shown = 'N/A';
  if (Ember.typeOf(metric) === 'number') {
    if (allowDecimal) {
      shown = (Number(metric) === 1 || Number(metric) === 0)
        ? metric * 100
        : (metric * 100).toFixed(1);
    } else {
      shown = metric;
    }
  }
  return shown;
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
export function enhanceAnomalies(rawAnomalies) {
  const newAnomalies = [];
  const anomaliesPresent = rawAnomalies.length;
  // De-dupe raw anomalies, extract only the good stuff (anomalyDetailsList)
  const anomalies = anomaliesPresent ? [].concat(...rawAnomalies.map(data => data.anomalyDetailsList)) : [];

/*  const pluralizeTime = (time, unit) => {
    const unitStr = time > 1 ? unit + 's' : unit;
    return time ? time + ' ' + unitStr : '';
  };*/

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
      }
      Object.assign(anomaly, { dimensionList });
    }

    newAnomalies.push(anomaly);
  }

  return newAnomalies;
}

export function setUpTimeRangeOptions(datesKeys, duration) {
  const newRangeArr = [];

  const defaultCustomRange = {
    name: 'Custom',
    value: 'custom',
    start: null,
    isActive: !datesKeys.includes(duration)
  }

  const dateKeyMap = new Map([
    [ '1m', ['Last 30 Days', 1, 'month'] ],
    [ '3m', ['3 Months', 3, 'month'] ],
    [ '2w', ['Last 2 Weeks', 2, 'week'] ],
    [ '1w', ['Last Week', 1, 'week'] ]
  ]);

  datesKeys.forEach((value) => {
    let currVal = dateKeyMap.get(value);
    let name = currVal[0];
    let start = moment().subtract(currVal[1], currVal[2]).endOf('day').utc()
    let isActive = duration === value;
    newRangeArr.push({ name, value, start, isActive });
  });

  newRangeArr.push(defaultCustomRange);

  return newRangeArr;
}

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @returns {Ember.RSVP promise}
 */
export function fetchCombinedAnomalies(anomalyIds) {
  const paginationDefault = 10;
  if (anomalyIds.length) {
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
  } else {
    return [];
  }
}

/**
 * Data needed to render the stats 'cards' above the anomaly graph for a given alert
 * @param {Object} alertEvalMetrics - contains the alert's performance data
 * @param {String} mode - the originating route
 * @returns {Array}
 */
export function buildAnomalyStats(alertEvalMetrics, mode) {
  const tooltip = false;

  const {
    totalAlerts: origTotal = 0,
    precision: origPrecision = 0,
    responseRate: origResponse,
    recall: origRecall,
    mttd: origMttd
  } = alertEvalMetrics.evalData;

  const {
    totalAlerts: newTotal = 0,
    precision: newPrecision = 0,
    responseRate: newResponse,
    recall: newRecall,
    mttd: newMttd
  } = alertEvalMetrics.projected;

  const responseRateObj = {
    title: 'Response Rate',
    units: '%',
    tooltip,
    text: '% of anomalies that are reviewed.',
    value: formatEvalMetric(origResponse),
    projected: 'none',
  };

  const anomalyStats = [
    {
      title: 'Number of anomalies',
      text: 'Estimated average number of anomalies',
      tooltip,
      value: formatEvalMetric(origTotal),
      projected: formatEvalMetric(newTotal)
    },
    {
      title: 'Precision',
      units: '%',
      tooltip,
      text: 'Among all anomalies detected, the % of them that are true.',
      value: formatEvalMetric(origPrecision),
      projected: formatEvalMetric(newPrecision),
    },
    {
      title: 'Recall',
      units: '%',
      tooltip,
      text: 'Among all anomalies that happened, the % of them detected by the system.',
      value: formatEvalMetric(origRecall, true),
      projected: formatEvalMetric(newRecall, true),
    },
    {
      title: 'MTTD for >30% change',
      units: 'hours',
      tooltip,
      text: 'Minimum time to detect for anomalies with > 30% change',
      value: formatEvalMetric(origMttd),
      projected: formatEvalMetric(newMttd)
    }
  ];

  if (mode === 'explore') {
    anomalyStats.splice(1, 0, responseRateObj);
  }

  return anomalyStats;
}

export default helper(
  formatEvalMetric,
  toIdGroups,
  pluralizeTime,
  enhanceAnomalies,
  setUpTimeRangeOptions,
  fetchCombinedAnomalies,
  buildAnomalyStats
);
