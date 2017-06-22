/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import Ember from 'ember';

export default Ember.Controller.extend({
  /**
   * Placeholder for patterns of interest options
   */
  patternsOfInterest: ['Pattern One', 'Pattern Two'],
  selectedPattern: 'Pattern One',
  /**
   * Placeholder for dimensions options
   */
  dimensionExploration: ['First', 'Second', 'Third'],
  selectedDimension: 'Select One',
  /**
   * Placeholder for alert groups options
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allAlertsConfigGroups'),
  selectedGroup: 'Select an alert group, or create new',
  /**
   * Actions for create alert form view
   */
  actions: {
    submit(data) {
      console.log(data);
    }
  }
});
