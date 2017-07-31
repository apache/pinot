import Ember from 'ember';
import moment from 'moment';
import { Actions } from 'thirdeye-frontend/actions/dimensions';

export default Ember.Route.extend({
  redux: Ember.inject.service(),
  model(params, transition) {
    const redux = this.get('redux');
    const { metricId } = transition.params['rca.details'];
    const {
      analysisStart: start,
      analysisEnd: end
    } = transition.queryParams;

    if (!metricId) { return; }

    redux.dispatch(Actions.fetchHeatMapData(Number(start), Number(end)));
    return {};
  },

  actions: {
    // Dispatches a redux action on query param change
    // to fetch heatmap data in the new date range
    queryParamsDidChange(changedParams, oldParams) {
      const redux = this.get('redux');
      let {
        analysisStart: start,
        analysisEnd: end
      } = changedParams;
      const params = Object.keys(changedParams || {});

      if (params.length && (start || end)) {

        start = start || oldParams.analysisStart;
        end = end || oldParams.analysisEnd;

        Ember.run.later(() => {
          redux.dispatch(Actions.fetchHeatMapData(
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
