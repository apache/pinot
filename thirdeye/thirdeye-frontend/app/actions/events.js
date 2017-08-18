import { type } from './utils';
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import {
  COMPARE_MODE_MAPPING,
  eventColorMapping,
  eventWeightMapping
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
  const color = eventColorMapping[eventType] || 'blue';

  return Object.assign(event, { color });
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
const assignEventTimeInfo = (event, anomalyStart, anomalyEnd, baselineStart, baselineEnd) => {
  const duration = event.end - event.start;
  const baselineOffset = baselineStart - anomalyStart;

  const relStart = event.start - anomalyStart;

  var relEnd = Infinity;
  var relDuration = Infinity;
  if (moment().isAfter(moment(event.end).add(1, 'minute'))) {
    relEnd = event.end - anomalyStart;
    relDuration = event.end - event.start;
  }

  var isBaseline = false;
  var displayStart = event.start;
  var displayEnd = event.end;
  if (displayStart < baselineEnd) {
    isBaseline = true;
    displayStart -= baselineOffset;
    displayEnd -= baselineOffset;
  }

  return Object.assign(event, { duration, relStart, relEnd, relDuration, isBaseline, displayStart, displayEnd });
};

/**
 * Helper function to offset Holiday dates to local timezone midnight rather than UTC
 */
const adjustHolidayTimestamp = (event) => {
  const startUtc = moment(event.start).utc();
  const endUtc = moment(event.start).utc();

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
      compareMode
    } = primaryMetric;
    const { events } = eventsState;

    if (!metricId || events.length) { return; }

    const analysisStart = currentStart;
    const analysisEnd = currentEnd;

    const diff = Math.floor((currentEnd - currentStart) / 4);
    const anomalyEnd = end || (+currentEnd - diff);
    const anomalyStart = start || (+currentStart + diff);
    mode = mode || compareMode;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(anomalyStart).clone().subtract(offset, 'week').valueOf();
    const baselineEnd = moment(anomalyEnd).clone().subtract(offset, 'week').valueOf();

    dispatch(setDate([anomalyStart, anomalyEnd]));
    dispatch(loading());
    return fetch(`/rootcause/query?framework=relatedEvents&anomalyStart=${anomalyStart}&anomalyEnd=${anomalyEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}&analysisStart=${analysisStart}&analysisEnd=${analysisEnd}&urns=thirdeye:metric:${metricId}`)
      .then(res => res.json())
      .then((res) => {
        return _.uniqBy(res, 'urn')
          .filter(event => event.eventType !== 'informed')
          .map(adjustHolidayTimestamp)
          .map(assignEventColor)
          .map(setWeight)
          .map(event => assignEventTimeInfo(event, anomalyStart, anomalyEnd, baselineStart, baselineEnd))
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
