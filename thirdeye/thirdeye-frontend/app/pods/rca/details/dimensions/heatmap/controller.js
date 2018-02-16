import Controller from '@ember/controller';

export default Controller.extend({
  // default heatmap Mode
  heatmapMode: 'Change in Contribution',
  dateFormat: 'MMM D, YYYY hh:mm a',
  heatmapModes: [
    'Percentage Change',
    'Change in Contribution',
    'Contribution to Overall Change'
  ]
});
