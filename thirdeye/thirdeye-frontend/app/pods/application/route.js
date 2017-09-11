import Ember from 'ember';

export default Ember.Route.extend({
  moment: Ember.inject.service(),
  beforeModel() {
    this.get('moment').setTimeZone('America/Los_Angeles');
  },

  model(params, transition) {
    const { targetName } = transition;
    
    // This is used to hide the navbar when accessing the screenshot page
    return targetName !== 'screenshot';
  }
});
