import Ember from 'ember';

export default Ember.Controller.extend({
  heatmapMode: 'Percentage Change',
  heatmapModes: [
    'Percentage Change',
    'Change in Contribution',
    'Contribution To Overall Change'
  ]
});
