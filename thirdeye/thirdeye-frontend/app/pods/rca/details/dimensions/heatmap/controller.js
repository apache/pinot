import Ember from 'ember';

export default Ember.Controller.extend({
  heatmapMode: 'Percentage Change',
  dateFormat: 'MMM D, YYYY hh:mm a',
  heatmapModes: [
    'Percentage Change',
    'Change in Contribution',
    'Contribution To Overall Change'
  ]
});
