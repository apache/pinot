import Route from '@ember/routing/route';
import applicationAnomalies from 'thirdeye-frontend/mocks/applicationAnomalies';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';

export default Route.extend({
  model() {
    return applicationAnomalies;
  },

  setupController(controller) {
    this._super(...arguments);
    controller.setProperties('columns', columns);
  }
});
