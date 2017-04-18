import Ember from 'ember';

export default Ember.Route.extend({
  model(){
    return Ember.$.getJSON('anomalies/search/time/1492326000000/1492412400000/1');
  }
});
