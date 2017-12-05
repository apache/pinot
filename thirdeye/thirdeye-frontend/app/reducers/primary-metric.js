import { ActionTypes } from '../actions/primary-metric';
import moment from 'moment';
import _ from 'lodash';

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

  primaryMetricId: null,
  relatedMetricEntities: {},
  selectedDimensions: [],
  selectedEvents: [],
  selectedMetricIds: [],
  regions: {},
  currentStart: null,
  currentEnd: moment().subtract(1, 'week').valueOf(),
  analysisStart: null,
  analysisEnd: null,
  filters: {},
  granularity: 'DAYS',
  compareMode: 'WoW',
  splitView: false,
  graphStart: null,
  graphEnd: null,
  isSelected: true,
  
  /**
   * Default color for primary Metric
   */
  color: 'blue'
};

const modeMap = {
  WoW: 1,
  Wo2W: 2,
  Wo3W: 3,
  Wo4W: 4
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOADING:
      return Object.assign({}, state, {
        loading: true,
        loaded: false,
        failed: false
      });

    case ActionTypes.LOAD_PRIMARY_METRIC: {
      let {
        id: primaryMetricId,
        startDate,
        endDate,
        filters = "{}",
        granularity,
        compareMode,
        graphStart,
        graphEnd,
        analysisStart,
        analysisEnd
      } = action.payload;

      // TODO: make this dryer
      startDate = startDate ? Number(startDate) : startDate;
      endDate = endDate? Number(endDate) : endDate;
      graphStart = graphStart ? Number(graphStart) : graphStart;
      graphEnd = graphEnd? Number(graphEnd) : graphEnd;
      analysisStart = analysisStart ? Number(analysisStart) : analysisStart;
      analysisEnd = analysisEnd? Number(analysisEnd) : analysisEnd;

      const offset = modeMap[compareMode] || 1;
      const baselineStart = moment(startDate).clone().subtract(offset, 'week').valueOf();
      const baselineEnd = moment(endDate).clone().subtract(offset, 'week').valueOf();

      return _.merge({}, state, {
        primaryMetricId,
        analysisStart,
        analysisEnd,
        currentStart: startDate,
        currentEnd: endDate,
        baselineStart,
        baselineEnd,
        filters,
        granularity,
        graphStart,
        graphEnd,
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
        .sort((prev, next) => next.score > prev.score)
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
        regions
      });
    }

    case ActionTypes.LOAD_DATA: {
      const relatedMetricEntities = Object.assign({}, action.payload);

      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        failed: false,
        relatedMetricEntities
      });
    }

    case ActionTypes.UPDATE_COMPARE_MODE: {
      const compareMode = action.payload;

      const offset = modeMap[compareMode] || 1;
      const baselineStart = moment(state.currentStart).clone().subtract(offset, 'week').valueOf();
      const baselineEnd = moment(state.currentEnd).clone().subtract(offset, 'week').valueOf();


      return Object.assign({}, state, {
        compareMode,
        baselineStart,
        baselineEnd
      });
    }

    case ActionTypes.UPDATE_DATES: {
      const {
        currentStart,
        currentEnd,
        analysisStart,
        analysisEnd
      } = action.payload;

      return _.merge(state, {
        currentStart,
        currentEnd,
        analysisStart,
        analysisEnd
      });
    }

    case ActionTypes.SELECT_PRIMARY: {
      const {isSelected} = state;
      return _.assign(state, { isSelected: !isSelected})
    }

    case ActionTypes.SELECT_DIMENSION: {
      const { selectedDimensions } = state;
      const dimension = action.payload;
      let newSelectedDimensions = [];

      if (selectedDimensions.includes(dimension)) {
        newSelectedDimensions =  selectedDimensions.filter((elem) =>  (elem !== dimension));
      } else {
        newSelectedDimensions = [...selectedDimensions, dimension];
      }

      return Object.assign({}, state, {
        selectedDimensions: newSelectedDimensions
      });
    }

    case ActionTypes.SELECT_EVENTS: {
      const { selectedEvents } = state;
      const event = action.payload;
      let newSelectedEvents = [];

      if (selectedEvents.includes(event)) {
        newSelectedEvents =  selectedEvents.filter((elem) =>  (elem !== event));
      } else {
        newSelectedEvents = [...selectedEvents, event];
      }

      return Object.assign({}, state, {
        selectedEvents: newSelectedEvents
      });
    }

    case ActionTypes.SELECT_METRICS: {

      const { selectedMetricIds } = state;
      const { metricId } = action.payload;
      let updatedMetricIds = [];

      if (selectedMetricIds.includes(metricId)) {
        updatedMetricIds =  selectedMetricIds.filter((id) =>  (id !== metricId));
      } else {
        updatedMetricIds = [...selectedMetricIds, metricId];
      }

      return Object.assign({}, state, {
        selectedMetricIds: updatedMetricIds
      });


    }

    case ActionTypes.RESET: {
      state = undefined;
    }
  }
  return state || INITIAL_STATE;
}
