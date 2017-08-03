import Ember from 'ember';

import { Actions } from 'thirdeye-frontend/actions/dimensions';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  // queryParam unique to the dimension route
  queryParams: {
    dimension: {
      replace: true,
      refreshModel: true
    }
  },

  model(params, transition) {
    const redux = this.get('redux');
    const { metricId } = transition.params['rca.details'];
    const {
      analysisStart: initStart,
      analysisEnd: initEnd
     } = this.modelFor('rca.details');

    const {
      dimension = 'All',
      analysisStart,
      analysisEnd
    } = transition.queryParams;

    const start = analysisStart || initStart;
    const end = analysisEnd || initEnd;

    if (!metricId) { return; }

    redux.dispatch(Actions.loading());
    redux.dispatch(Actions.updateDates(
      Number(start),
      Number(end)
    ));

    Ember.run.later(() => {
      redux.dispatch(Actions.updateDimension(dimension)).then(() => {
        redux.dispatch(Actions.fetchDimensions(metricId));
      });
    });

    return {};
  },

  actions: {
    // Dispatches a redux action on query param change
    // to fetch events in the new date range
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
