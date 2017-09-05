import Ember from 'ember';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Route.extend({
  model(params) {
    const { alertId: id } = params;
    if (!id) { return; }

    const url = `onboard/function/${id}`;
    return fetch(url).then(checkStatus);
  }

  // afterModel() {

  // },
});
