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
  const weight = eventWeightMapping[eventType] || 0;

  return Object.assign(event, {score: weight});
};

/**
 * Helper function assigning start and end times to events relative
 * to the anomaly period's start time
 */
const assignEventTimeInfo = (anomalyStart, event) => {
  const relStart = event.start - anomalyStart;

  var relEnd = 'ongoing';
  var duration = 'ongoing';
  if (moment().isAfter(moment(event.end).add(1, 'minute'))) {
    relEnd = event.end - anomalyStart;
    duration = event.end - event.start;
  }

  return Object.assign(event, { relStart, relEnd, duration });
};

/**
 * Helper function to humanize a moment
 */
const humanizeShort = (dur) => {
  return dur.humanize().replace('minute', 'min');
};

/**
 * Helper function to add human-readable time labels as record properties
 */
const assignHumanTimeInfo = (event) => {
  var humanRelStart = event.relStart;
  if (event.relStart >= 0) {
    const out = humanizeShort(moment.duration(event.relStart));
    humanRelStart = `${out} after`;
  } else {
    const out = humanizeShort(moment.duration(-1 * event.relStart));
    humanRelStart = `${out} before`;
  }

  var humanDuration = event.duration;
  if (!Number.isNaN(event.duration)) {
    humanDuration = humanizeShort(moment.duration(event.duration));
  }

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
    const { primaryMetric } = getState();

    const {
      primaryMetricId: metricId,
      currentEnd = moment().subtract(1, 'day').endOf('day').valueOf(),
      currentStart = moment(currentEnd).subtract(1, 'week').valueOf(),
      compareMode
    } = primaryMetric;

    if (!metricId) {return;}

    const diff = Math.floor((currentEnd - currentStart) / 4);
    const anomalyEnd = end || (+currentEnd - diff);
    const anomalyStart = start || (+currentStart + diff);
    mode = mode || compareMode;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(anomalyStart).clone().subtract(offset, 'week').valueOf();
    const baselineEnd = moment(anomalyEnd).clone().subtract(offset, 'week').valueOf();

    dispatch(setDate([anomalyStart, anomalyEnd]));
    dispatch(loading());
    return fetch(`/rootcause/query?framework=relatedEvents&anomalyStart=${anomalyStart}&anomalyEnd=${anomalyEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}&analysisStart=${currentStart}&analysisEnd=${currentEnd}&urns=thirdeye:metric:${metricId}`)
      .then(res => res.json())
      .then((res) => {
        // hidding informed events

        return _.uniqBy(res, 'urn')
          .filter(event => event.eventType !== 'informed')
          .map(assignEventColor)
          .map(setWeight)
          .map(event => assignEventTimeInfo(anomalyStart, event))
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

    return dispatch(fetchEvents(start, end, compareMode));
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
