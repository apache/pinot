import Ember from 'ember';

export default Ember.Controller.extend({
  // default heatmap Mode
  heatmapMode: 'Contribution to Overall Change',
  dateFormat: 'MMM D, YYYY hh:mm a',
  heatmapModes: [
    'Percentage Change',
    'Change in Contribution',
    'Contribution to Overall Change'
  ]
});
