import { ActionTypes } from '../actions/anomaly';

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
   * List the anomaly ids in order
   */
  ids: [],

  /**
   * Items in hash map
   */
  entities: {}
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOAD: {
      const anomalyList = action.payload.anomalyDetailsList;
      const ids = anomalyList.map((anomaly) => anomaly.anomalyId);
      const entities = anomalyList.reduce((entities, anomaly) => {
        entities[anomaly.anomalyId] = anomaly;
        return entities;
      }, {});

      return Object.assign(state, {
        loading: false,
        loaded: true,
        ids,
        entities,
      });
    }
    case ActionTypes.LOADING:
      return Object.assign(state, {
        loading: true,
        loaded: false
      });

    case ActionTypes.REQUEST_FAIL:
      return Object.assign(state, {
        loading: false,
        failed: true
      });
  }
  return state;
}
