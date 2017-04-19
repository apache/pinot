import Ember from 'ember';
import fetch from 'ember-network/fetch';

export default Ember.Route.extend({
  model(){
    return fetch('anomalies/search/time/1492326000000/1492412400000/1')
      .then(res => res.json());
  }
});
