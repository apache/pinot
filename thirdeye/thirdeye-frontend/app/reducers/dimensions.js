import { ActionTypes } from '../actions/dimensions';
import { colors } from '../actions/constants';

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

  keys: [],
  metricId: null,
  dimensions: {},
  timeseries: [],
  selectedDimension: 'All',
  heatmapData: {}
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOAD: {
      const keys = action.payload || [];
      const { metricId } = action;

      return Object.assign({}, state, {
        keys,
        metricId,
        loading: false,
        loaded: true,
        failed: false
      });
    }

    case ActionTypes.LOADING:
      return Object.assign({}, state, {
        loading: true,
        loaded: false,
        failed: false
      });

    case ActionTypes.SET: {
      const selectedDimension = action.payload;
      return Object.assign({}, state, {
        selectedDimension
      });
    }


    case ActionTypes.LOAD_HEATMAP: {
      const heatmapData = _.merge({}, action.payload);

      return Object.assign({}, state, { heatmapData });
    }

    case ActionTypes.LOAD_TIMESERIES: {
      const timeseries = action.payload;
      const { subDimensionContributionMap: subdimensionMap } = timeseries;
      const { selectedDimension } = state;

      const dimensions = Object.keys(subdimensionMap)
        .reduce((hash, subdimension, index) => {
          const subdimensionData = _.merge(
            {
              name: subdimension,
              dimension: selectedDimension,
              id: `${selectedDimension}-${subdimension}`,
              color: colors[index % colors.length]
            },
            subdimensionMap[subdimension]);

          hash[`${selectedDimension}-${subdimension}`] = subdimensionData;

          return hash;
        }, {});

      return Object.assign({}, state, {
        timeseries,
        dimensions: Object.assign({}, state.dimensions, dimensions),
        loaded: true,
        loading: false,
        failed: false
      });
    }

    case ActionTypes.REQUEST_FAIL:
      return Object.assign({}, state, {
        loaded: false,
        loading: false,
        failed: true
      });

    case ActionTypes.RESET: {
      state = undefined;
    }
  }
  return state || INITIAL_STATE;
}
