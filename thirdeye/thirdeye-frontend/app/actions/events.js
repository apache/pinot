import { type } from './utils';
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import {
  COMPARE_MODE_MAPPING,
  eventColorMapping,
  baselineEventColorMapping,
  eventWeightMapping,
  ROOTCAUSE_ANALYSIS_DURATION_MAX,
  ROOTCAUSE_ANOMALY_DURATION_MAX
} from './constants';

/**
 * Define the metric action types
 */
export const ActionTypes = {
  LOADING: type('[Events] Loading'),
  LOADED: type('[Events] Data loaded'),
  LOAD_EVENTS: type('[Events] Load events'),
  SET_DATE: type('[Events] Set new region dates'),
  REQUEST_FAIL: type('[Events] Request Fail'),
  RESET: type('[Event] Reset Data')
};

function resetData() {
  return {
    type: ActionTypes.RESET
  };
}

function loading() {
  return {
    type: ActionTypes.LOADING
  };
}

function loadEvents(response) {
  return {
    type: ActionTypes.LOAD_EVENTS,
    payload: response
  };
}

/**
 * Set Events Status to loaded
 */
function loaded() {
  return {
    type: ActionTypes.LOADED
  };
}

function setDate(dates) {
  return {
    type: ActionTypes.SET_DATE,
    payload: dates
  };
}

/**
 * Helper function assigning colors to events based on the type
 * @param {Object} event The event object
 */
const assignEventColor = (event) => {
  const { eventType } = event;
  let color = eventColorMapping[eventType] || 'blue';
  let displayColor = color;
  if (event.isBaseline && (['holiday', 'gcn'].includes(event.eventType))) {
    displayColor = baselineEventColorMapping[eventType];
  }

  return Object.assign(event, { color, displayColor });
};

/**
 * Helper function assigning weight to events based on the type
 * @param {Object} event The event object
 */
const setWeight = (event) => {
  const { eventType } = event;
  const displayScore = eventWeightMapping[eventType] || 0;

  return Object.assign(event, { displayScore });
};

/**
 * Helper function assigning start and end times to events relative
 * to the anomaly period's start time
 */
const assignEventTimeInfo = (event, anomalyStart, anomalyEnd, baselineStart, baselineEnd, analysisStart, analysisEnd) => {
  const duration = event.end - event.start;
  const baselineOffset = baselineStart - anomalyStart;

  const relStart = event.start - anomalyStart;

  let relEnd = Infinity;
  let relDuration = Infinity;
  if (event.end > 0 && moment().isAfter(moment(event.end).add(1, 'minute'))) {
    relEnd = event.end - anomalyStart;
    relDuration = event.end - event.start;
  }

  let isBaseline = false;
  let isFuture = false;
  let isOngoing = false;
  let {
    start: displayStart,
    end: displayEnd,
    label: displayLabel
  } = event;

  // baseline event (anomaly, holiday & gcn only)
  if (['anomaly', 'holiday', 'gcn'].includes(event.eventType)) {
    if ((baselineStart < displayEnd && displayStart < baselineEnd)) {
      isBaseline = true;
      displayStart -= baselineOffset;
      displayEnd -= baselineOffset;
      displayLabel += " (baseline)";
    }
  }

  // upcoming event
  if (displayStart >= analysisEnd) {
    isFuture = true;
    displayStart = moment(analysisEnd).subtract(1, 'hour').valueOf();
    displayEnd = analysisEnd;
    displayLabel += " (upcoming)";
  }

  // ongoing event
  if (displayEnd <= 0) {
    isOngoing = true;
    displayEnd = analysisEnd;
  }

  return Object.assign(event, { duration, relStart, relEnd, relDuration, isBaseline, isFuture, isOngoing, displayStart, displayEnd, displayLabel });
};

/**
 * Helper function to offset Holiday dates to local timezone midnight rather than UTC
 */
const adjustHolidayTimestamp = (event) => {
  const startUtc = moment(event.start).utc();
  const endUtc = moment(event.end).utc();

  if (event.eventType == 'holiday') {
    if (startUtc.isSame(startUtc.startOf('day'))) {
      const utcOffset = moment().utcOffset();
      const start = startUtc.add(-utcOffset, 'minutes').valueOf();
      const end = endUtc.add(-utcOffset, 'minutes').valueOf();

      return Object.assign(event, { start, end });
    }
  }

  return event;
};

/**
 * Helper function to append localized date/time to deployment event label
 */
const adjustInformedLabel = (event) => {
  if (event.eventType === 'informed') {
    const displayLabel = event.displayLabel + ' (' + moment(event.start).format('hh:mm a') + ')';
    return Object.assign(event, { displayLabel });
  }
  return event;
};

/**
 * Helper function to adjust investigation link for legacy RCA/anomaly page
 */
const adjustAnomalyLink = (event) => {
  if (event.eventType === 'anomaly') {
    const anomalyId = event.urn.split(':')[3];
    const link = `/thirdeye#investigate?anomalyId=${anomalyId}`;
    return Object.assign(event, { link });
  }
  return event;
};

/**
 * Helper function to humanize a start offset
 */
const humanizeStart = (offset) =>
{
  const dur = moment.duration(Math.abs(offset));

  let out = 'just';
  if (dur >= moment.duration(1, 'minute')) {
    out = dur.humanize().replace('minute', 'min').replace('second', 'sec');
  }

  if (offset >= 0) {
    return `${out} after`;
  } else {
    return `${out} before`;
  }
};

/**
 * Helper function to humanize a duration
 */
const humanizeDuration = (duration) =>
{
  if (!Number.isFinite(duration)) {
    return 'ongoing';
  }

  const dur = moment.duration(duration);
  if (dur <= moment.duration(1, 'minute')) {
    return '';
  }

  return dur.humanize().replace('minute', 'min').replace('second', 'sec');
};

/**
 * Helper function to add human-readable time labels as record properties
 */
const assignHumanTimeInfo = (event) => {
  const humanRelStart = humanizeStart(event.relStart);
  const humanDuration = humanizeDuration(event.relDuration);

  return Object.assign(event, { humanRelStart, humanDuration });
};

/**
 * Fetches all events based for a metric
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 * @param {string} mode  The compare mode
 */
function fetchEvents(start, end, mode) {
  return (dispatch, getState) => {
    const {
      primaryMetric,
      events: eventsState
    } = getState();

    const {
      primaryMetricId: metricId,
      currentEnd = moment().subtract(1, 'day').endOf('day').valueOf(),
      currentStart = moment(currentEnd).subtract(1, 'week').valueOf(),
      compareMode,
      filters: filterJson
    } = primaryMetric;
    const { events } = eventsState;

    if (!metricId || events.length) { return; }

    const analysisStartRaw = currentStart;
    const analysisEndRaw = currentEnd;

    const analysisDurationSafe = Math.min(analysisEndRaw - analysisStartRaw, ROOTCAUSE_ANALYSIS_DURATION_MAX);
    const analysisStart = Math.max(analysisStartRaw, analysisEndRaw - analysisDurationSafe);
    const analysisEnd = analysisEndRaw;

    const diff = Math.floor((currentEnd - currentStart) / 4);
    const anomalyEndRaw = end || (+currentEnd - diff);
    const anomalyStartRaw = start || (+currentStart + diff);
    mode = mode || compareMode;

    const anomalyDurationSafe = Math.min(anomalyEndRaw - anomalyStartRaw, ROOTCAUSE_ANOMALY_DURATION_MAX);
    const anomalyStart = anomalyStartRaw;
    const anomalyEnd = anomalyStartRaw + anomalyDurationSafe;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(anomalyStart).clone().subtract(offset, 'week').valueOf();
    const baselineEnd = moment(anomalyEnd).clone().subtract(offset, 'week').valueOf();

    const filters = JSON.parse(filterJson);
    const filterUrns = Object.keys(filters).map(key => 'thirdeye:dimension:' + key + ':' + filters[key] + ':provided');
    const urns = [`thirdeye:metric:${metricId}`].concat(filterUrns).join(',');

    dispatch(setDate([anomalyStart, anomalyEnd]));
    dispatch(loading());
    return fetch(`/rootcause/query?framework=relatedEvents&anomalyStart=${anomalyStart}&anomalyEnd=${anomalyEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}&analysisStart=${analysisStart}&analysisEnd=${analysisEnd}&urns=${urns}`)
      .then(res => res.json())
      .then((res) => {
        return _.uniqBy(res, 'urn')
          .map(adjustHolidayTimestamp)
          .map(event => assignEventTimeInfo(event, anomalyStart, anomalyEnd, baselineStart, baselineEnd, analysisStart, analysisEnd))
          .map(adjustInformedLabel)
          .map(adjustAnomalyLink)
          .map(assignEventColor)
          .map(setWeight)
          .map(assignHumanTimeInfo)
          .sort((a, b) => (b.score - a.score));
      })
      .then(res => dispatch(loadEvents(res)
      ));
  };
}

/**
 * Updates the date range for the events and refetches the data
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 * @param {string} mode  The compare mode
 */
function updateDates(start, end, compareMode) {
  return (dispatch, getState) => {
    const { primaryMetric } = getState();
    compareMode = compareMode || primaryMetric.compareMode;
    return dispatch(reset())
      .then(() => dispatch(fetchEvents(start, end, compareMode)));
  };
}

// Resets the store to its initial state
function reset() {
  return (dispatch) => {
    dispatch(resetData());
    return Promise.resolve();
  };
}

export const Actions = {
  loading,
  loaded,
  loadEvents,
  fetchEvents,
  updateDates,
  reset
};
