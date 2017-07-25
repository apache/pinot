import Ember from 'ember';
import moment from 'moment';
import { Actions } from 'thirdeye-frontend/actions/events';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  /**
   * Massages Query Params from URL and dispatch redux actions
   */
  model(params, transition) {
    const redux = this.get('redux');
    const { metricId } = transition.params['rca.details'];
    const {
      analysisStart: start,
      analysisEnd: end
    } = transition.queryParams;

    if (!metricId) { return; }

    redux.dispatch(Actions.fetchEvents(Number(start), Number(end)));
    return {};

  },

  actions: {
    // Dispatches a redux action on query param change
    // to fetch events in the new date range
    queryParamsDidChange(changedParams) {

      const redux = this.get('redux');
      const {
        analysisStart: start,
        analysisEnd: end
      } = changedParams;
      const params = Object.keys(changedParams || {});

      if (params.length === 2 && start && end) {
        Ember.run.later(() => {
          redux.dispatch(Actions.updateDates(
            Number(start),
            Number(end)
          ));
        });
      }
      this._super(...arguments);

      return true;
    }
  }
});
