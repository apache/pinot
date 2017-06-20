import Ember from 'ember';
// import fetch from 'fetch';
import { Alerts as AlertGroups } from 'thirdeye-frontend/actions/alerts';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  model(params) {
    const redux = this.get('redux');

    redux.dispatch(AlertGroups.fetchData())
    return {};
  }
});

