import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
// import fetch from 'fetch';
import { Actions as AnomalyActions } from 'thirdeye-frontend/actions/anomaly';

export default Route.extend({
  redux: service(),

  model(params) {
    const { id } = params;
    const redux = this.get('redux');

    redux.dispatch(AnomalyActions.fetchData(id));
    return {};
  }
});

