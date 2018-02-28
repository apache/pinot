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
   * Retrieves metric and alert lists to index anomalies
   * @type {Array} - array of array, where the first element is the metric list and the second is the alert list
   * @example
   * [
   *  ["metric 1", "metric 2"],
   *  ["alert 1", "alert 2"]
   * ]
   */
  getMetricAlertLists() {
    let metricList = new Set();
    let alertList = new Set();

    applicationAnomalies.forEach(anomaly => {
      metricList.add(anomaly.metric);
      alertList.add(anomaly.functionName);
    });

    return [ [...metricList], [...alertList] ];
  },

  setupController(controller) {
    this._super(...arguments);

    controller.setProperties({
      columns,
      metricList: this.getMetricAlertLists()[0],
      alertList: this.getMetricAlertLists()[1]
    });
  }
});
