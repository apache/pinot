import { ActionTypes } from '../actions/anomaly';
import moment from 'moment';
/**
 * Define the schema
 */
const INITIAL_STATE = {

  /**
   * State for loading
   */
  loading: false,

  /**
   * State for loaded
   */
  loaded: false,

  /**
   * State for failed request
   */
  failed: false,

  /**
   * The primary metric Id
   */
  id: null,

  /**
   * The primary anomaly
   */
  entity: {},

  /**
   * filters
   */
  filters: {},

  /**
   * anomaly Start date
   */
  startDate: moment(),

  /**
   * anomaly end date
   */
  endDate: moment(),

  /**
   *
   */
  granularity: 'DAYS'
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOAD: {
      const entity = action.payload.pop() || {};
      const {
        metricId,
        currentStart,
        currentEnd,
        anomalyRegionStart,
        anomalyRegionEnd,
        anomalyFunctionDimension,
        timeUnit
      } = entity;

      const offset = {
        'MINUTES': 3,
        'HOURS': 24,
        'DAY': 72
      }[timeUnit] || 3;

      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        failed: false,
        id: metricId,
        entity,
        primaryMetricId: metricId,
        granularity: timeUnit,
        filters: anomalyFunctionDimension,
        currentEnd: moment(currentEnd).add(offset, 'hours').valueOf(),
        currentStart: moment(currentStart).subtract(offset, 'hours').valueOf(),
        anomalyRegionStart: moment(anomalyRegionStart).valueOf(),
        anomalyRegionEnd: moment(anomalyRegionEnd).valueOf()
      });
    }
    case ActionTypes.LOADING:
      return Object.assign({}, state, {
        loading: true,
        loaded: false,
        failed: false
      });

    case ActionTypes.REQUEST_FAIL:
      return Object.assign({}, state, {
        loading: false,
        failed: true
      });
  }

  return state;
}
