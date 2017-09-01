import Ember from 'ember';
import fetch from 'fetch';

export default Ember.Route.extend({
  model(params) {
    const { alertId: id } = params;
    if (!id) { return; }

    const url = `onboard/function/${id}`;
    return fetch(url).then(res => res.json());
  }
});
