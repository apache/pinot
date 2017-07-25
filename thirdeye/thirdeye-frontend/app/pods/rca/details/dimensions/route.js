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
      dimension = 'All'
    } = transition.queryParams;

    if (!metricId) { return; }

    redux.dispatch(Actions.loading());
    Ember.run.later(() => {
      redux.dispatch(Actions.updateDimension(dimension)).then(() => {
        redux.dispatch(Actions.fetchDimensions(metricId));
      });
    });

    return {};
  }
});
