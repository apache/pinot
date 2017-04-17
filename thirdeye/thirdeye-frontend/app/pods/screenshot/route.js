import Ember from 'ember';

export default Ember.Route.extend({
  model(){
    return Ember.$.getJSON('/anomalies/search/time/1492326000000/1492412400000/1');
    // return Ember.$.getJSON('http://localhost:1426/anomalies/search/time/1492326000000/1492412400000/1');
  }
});
