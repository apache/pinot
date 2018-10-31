import { ActionTypes } from '../actions/events';

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
  eventStart: '',
  eventEnd: '',
  events: []
};

export default function reducer(state = INITIAL_STATE, action = {}) {
  switch (action.type) {
    case ActionTypes.LOADING:
      return Object.assign({}, state, {
        loading: true,
        loaded: false,
        failed: false
      });

    case ActionTypes.LOAD_EVENTS: {
      const events = action.payload || [];
      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        failed: false,
        events
      });
    }

    case ActionTypes.LOADED: {
      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        failed: false
      });
    }

    case ActionTypes.SET_DATE: {
      const [ eventStart, eventEnd ] = action.payload;

      return Object.assign({}, state, {
        eventStart,
        eventEnd
      });
    }

    case ActionTypes.RESET: {
      state = undefined;
    }

  }
  return state || INITIAL_STATE;
}
