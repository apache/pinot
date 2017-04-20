import Ember from 'ember';
import fetch from 'ember-network/fetch';
import { Actions as AnomalyActions } from 'thirdeye-frontend/actions/anomaly';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  model(params){
    const { id } = params;
    const redux = this.get('redux');

    redux.dispatch(AnomalyActions.loading());
    fetch(`anomalies/search/anomalyIds/1492498800000/1492585200000/1?anomalyIds=${id}&functionName=`)
      .then(res => res.json())
      .then(response => redux.dispatch(AnomalyActions.loadAnomaly(response)))
      .catch(() => redux.dispatch(AnomalyActions.requestFail()));

    return {};
  }
});


