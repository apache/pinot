import Ember from 'ember';
// import fetch from 'fetch';
import { Actions as AnomalyActions } from 'thirdeye-frontend/actions/anomaly';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  model(params) {
    const { id } = params;
    const redux = this.get('redux');

    redux.dispatch(AnomalyActions.fetchData(id));
    return {};
  }
});

