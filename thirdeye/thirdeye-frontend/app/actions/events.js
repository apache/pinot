import { type } from './utils';
import fetch from 'fetch';
import moment from 'moment';
import { COMPARE_MODE_MAPPING, eventColorMapping } from './constants';

/**
 * Define the metric action types
 */
export const ActionTypes = {
  LOADING: type('[Events] Loading'),
  LOADED: type('[Events] Data loaded'),
  LOAD_EVENTS: type('[Events] Load events'),
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

/**
 * Helper function assigning colors to events based on the type
 * @param {Object} event The event object
 */
const assignEventColor = (event) => {
  const color = eventColorMapping[event.eventType];

  return Object.assign(event, { color });
};


/**
 * Fetches all events based for a metric
 * @param {Number} start The start time in unix ms
 * @param {Number} end The end time in unix ms
 * @param {string} mode  The compare mode
 */
function fetchEvents(start, end, mode) {
  return (dispatch, getState) => {
    const { primaryMetric, events } = getState();

    const {
      primaryMetricId: metricId,
      currentStart = moment(endDate).subtract(1, 'week').valueOf(),
      currentEnd =  moment().subtract(1, 'day').endOf('day').valueOf(),
      compareMode
    } = primaryMetric;

    const diff = Math.floor((currentEnd - currentStart) / 4);
    const endDate = end || (+currentEnd - diff);
    const startDate = start || (+currentStart + diff);
    mode = mode || compareMode;

    const offset = COMPARE_MODE_MAPPING[compareMode] || 1;
    const baselineStart = moment(startDate).clone().subtract(offset, 'week').valueOf();
    const windowSize = Math.max(endDate - startDate, 1);

    dispatch(loading());
    return fetch(`/rootcause/query?framework=relatedEvents&current=${startDate}&baseline=${baselineStart}&windowSize=${windowSize}&urns=thirdeye:metric:${metricId}`)
      .then(res => res.json())
      .then((res) => {
        // hidding informed events
        return res
          .filter(event => event.eventType !== 'informed')
          .map(assignEventColor)
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
    if (primaryMetric.selectedEvents.length) { return;}

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
