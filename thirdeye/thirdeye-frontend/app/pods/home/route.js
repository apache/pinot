import Route from '@ember/routing/route';
import applicationAnomalies from 'thirdeye-frontend/mocks/applicationAnomalies';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';

export default Route.extend({

  /**
   * Returns a two-dimensional array, which maps anomalies by metric and functionName (aka alert)
   * @example
   * [
   *  "Metric 1": [
   *    "Alert 1": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 11": [ {anomalyObject}, {anomalyObject} ]
   *  ],
   *  "Metric 2": [
   *    "Alert 21": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 22": [ {anomalyObject}, {anomalyObject} ]
   *   ]
   * ]
   */
  model() {
    let anomalyMapping = [];

    applicationAnomalies.forEach(anomaly => {
      const { metric, functionName } = anomaly;

      if (!anomalyMapping[metric]) {
        anomalyMapping[metric] = [];
      }

      if (!anomalyMapping[metric][functionName]) {
        anomalyMapping[metric][functionName] = [];
      }

      anomalyMapping[metric][functionName].push(anomaly);
    });

    return anomalyMapping;
  },

  setupController(controller) {
    this._super(...arguments);
    controller.set('columns', columns);
  }
});
