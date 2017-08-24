import { ActionTypes } from '../actions/metrics';
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
   * Lost of related Metric
   */
  relatedMetricIds: [],

  selectedMetricIds: [],
  primaryMetricId: null,
  relatedMetricEntities: {},
  regions: {},
  currentStart: null,
  currentEnd: moment().subtract(1, 'week').valueOf(),
  filters: {},
  granularity: 'DAYS',
  compareMode: 'WoW',
  splitView: false,
  regionStart: '',
  regionEnd: ''
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOADING:
      return Object.assign({}, state, {
        loading: true,
        loaded: false,
        failed: false
      });

    case ActionTypes.LOADED: {
      return Object.assign({}, state, {
        loaded: true,
        loading: false,
        failed: false
      });
    }

    case ActionTypes.LOAD_PRIMARY_METRIC: {
      let {
        primaryMetricId,
        startDate,
        endDate,
        filters = "{}",
        granularity,
        compareMode
      } = action.payload;

      startDate = Number(startDate);
      endDate = Number(endDate);

      return Object.assign({}, state, {
        primaryMetricId,
        currentStart: startDate,
        currentEnd: endDate,
        filters,
        granularity,
        compareMode,
        loading: true,
        loaded: false,
        failed: false
      });
    }

    case ActionTypes.REQUEST_FAIL:
      return Object.assign({}, state, {
        loading: false,
        failed: true
      });

    case ActionTypes.LOAD_IDS: {
      const relatedMetrics = action.payload;
      const relatedMetricIds = relatedMetrics
        .sortBy('score').reverse()
        .map((metric) => Number(metric.urn.split('thirdeye:metric:').pop()));

      return Object.assign({}, state,  {
        relatedMetricIds
      });
    }

    case ActionTypes.LOAD_REGIONS: {
      const payload = action.payload;
      const regions = Object.keys(payload).reduce((aggr, id) => {
        aggr[id] = {regions: payload[id]};
        return aggr;
      }, {});

      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        failed: false,
        regions
      });
    }

    case ActionTypes.LOAD_DATA: {
      const relatedMetricEntities = Object.assign({}, action.payload);

      return Object.assign({}, state, {
        relatedMetricEntities
      });
    }

    case ActionTypes.UPDATE_COMPARE_MODE: {
      const compareMode = action.payload;

      return Object.assign({}, state, {
        compareMode
      });
    }

    case ActionTypes.UPDATE_DATE: {
      const { currentStart, currentEnd } = action.payload;

      return Object.assign({}, state, {
        currentStart,
        currentEnd,
        loading: true,
        loaded: false,
        failed: false
      });
    }

    case ActionTypes.SELECT_METRIC: {
      const { selectedMetricIds } = action.payload;

      return Object.assign({}, state, {
        selectedMetricIds
      });
    }

    case ActionTypes.SET_DATE: {
      const [ regionStart, regionEnd ] = action.payload;

      return Object.assign({}, state, {
        regionStart,
        regionEnd
      });
    }

    case ActionTypes.RESET: {
      state = undefined;
    }
  }
  return state || INITIAL_STATE;
}
