import Ember from 'ember';
import primaryMetric from 'thirdeye-frontend/mocks/primaryMetric';
import events from 'thirdeye-frontend/mocks/sampleEvents';
import filterBarConfig from 'thirdeye-frontend/mocks/filterBarConfig';

export default Ember.Route.extend({
  model() {
    return {
      primaryMetric,
      events,
      filterBarConfig
    };
  },

  /**
   * sets the model and filterBlocks in the controller
   * sets filterBlocks to be built based on filter bar config
   * @param {Object} controller
   * @param {Object} model
   */
  setupController(controller, model) {
    const filterBlocks = model.filterBarConfig;
    controller.setProperties({
      model,
      filterBlocks
    });
  }
});
