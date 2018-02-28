import Route from '@ember/routing/route';
import applicationAnomalies from 'thirdeye-frontend/mocks/applicationAnomalies';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';

export default Route.extend({

  /**
   * Returns a two-dimensional array, which maps anomalies by metric and functionName (aka alert)
   * @return {Array.<Array.<Object>>}
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
      const { metric, functionName, current, baseline } = anomaly;
      const value = {
        current,
        baseline
      };

      if (!anomalyMapping[metric]) {
        anomalyMapping[metric] = [];
      }

      if (!anomalyMapping[metric][functionName]) {
        anomalyMapping[metric][functionName] = [];
      }

      anomalyMapping[metric][functionName].push(anomaly);
      anomaly.value = value;
      anomaly.investigationLink = `/rootcause?anomalyId=${anomaly.id}`;
    });

    return anomalyMapping;
  },

  /**
   * Retrieves metrics to index anomalies
   * @return {String[]} - array of strings, each of which is a metric
   */
  getMetrics() {
    let metricSet = new Set();

    applicationAnomalies.forEach(anomaly => {
      metricSet.add(anomaly.metric);
    });

    return [...metricSet];
  },

  /**
   * Retrieves alerts to index anomalies
   * @return {String[]} - array of strings, each of which is a alerts
   */
  getAlerts() {
    let alertSet = new Set();
    applicationAnomalies.forEach(anomaly => {
      alertSet.add(anomaly.functionName);
    });

    return [...alertSet];
  },

  /**
   * Sets the table column, metricList, and alertList
   * @return {undefined}
   */
  setupController(controller) {
    this._super(...arguments);

    controller.setProperties({
      columns,
      metricList: this.getMetrics(),
      alertList: this.getAlerts()
    });
  }
});
