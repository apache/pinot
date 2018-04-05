import Route from '@ember/routing/route';
import { humanizeFloat, humanizeChange, checkStatus } from 'thirdeye-frontend/utils/utils';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';
import fetch from 'fetch';
import { hash } from 'rsvp';
import {
  getFormatedDuration,
  getAnomaliesByAppName,
  anomalyResponseObj,
  getPerformanceByAppNameUrl
} from 'thirdeye-frontend/utils/anomaly';
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';
import { setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import RSVP from "rsvp";
import moment from 'moment';

const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Route.extend({
  queryParams: {
    appName: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },
  applicationAnomalies: null,
  appName: null,
  startDate: moment().startOf('day').utc().valueOf(),
  endDate: moment().utc().valueOf(),

  /**
   * Returns a mapping of anomalies by metric and functionName (aka alert), performance stats for anomalies by
   * application, and redirect links to the anomaly search page for each metric-alert mapping
   * @return {Object}
   * @example
   * {
   *  "Metric 1": {
   *    "Alert 1": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 11": [ {anomalyObject}, {anomalyObject} ]
   *  },
   *  "Metric 2": {
   *    "Alert 21": [ {anomalyObject}, {anomalyObject} ],
   *    "Alert 22": [ {anomalyObject}, {anomalyObject} ]
   *   }
   * }
   */
  model(params) {
    const { appName, startDate, endDate } = params;
    return hash({
      appName,
      startDate,
      endDate,
      applications: fetch(selfServeApiCommon.allApplications).then(checkStatus)
    });
  },

  afterModel(model) {
    const appName = model.appName || model.applications[0].application;
    const startDate = Number(model.startDate) || this.get('startDate');
    const endDate = Number(model.endDate) || this.get('endDate');

    this.setProperties({
      appName,
      startDate,
      endDate
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = await this.get('_getAnomalyMapping').perform(model);
        const anomalyPerformance = await getPerformanceByAppNameUrl(appName, moment(this.get('startDate')).startOf('day').utc().format(), moment(this.get('endDate')).startOf('day').utc().format());
        const defaultParams = {
          anomalyMapping,
          anomalyPerformance
        };
        resolve(Object.assign(model, { ...defaultParams }));
      } catch (error) {
        reject(new Error('Unable to retrieve anomaly data. ${error}'));
      }
    });
  },

  _getAnomalyMapping: task (function * (model) {//TODO: need to add to anomaly util - LH
    let anomalyMapping = {};
    let redirectLink = {};
    //fetch the anomalies
    const applicationAnomalies = yield getAnomaliesByAppName(this.get('appName'), this.get('startDate'));
    this.set('applicationAnomalies', applicationAnomalies);
    try {
      this.get('applicationAnomalies').forEach(anomaly => {
        const { metricName, functionName, current, baseline, metricId } = anomaly;

        if (!anomalyMapping[metricName]) {
          anomalyMapping[metricName] = [];
        }

        // if (!anomalyMapping[metric][functionName]) {//TODO: not used now. Clean up to follow - lohuynh
        //   anomalyMapping[metric][functionName] = [];
        // }

        if(!redirectLink[metricName]) {
          redirectLink[metricName] = {};
        }

        // if (!redirectLink[metricName][functionName]) {//TODO: not used now. Clean up to follow - lohuynh
        //   // TODO: Once start/end times are introduced, add these to the link below
        //   redirectLink[metricName][functionName] = `/thirdeye#anomalies?anomaliesSearchMode=metric&pageNumber=1&metricId=${metricId}\
        //                         &searchFilters={"functionFilterMap":["${functionName}"]}`;
        // }

        // Format current and baseline numbers, so numbers in the millions+ don't overflow
        const anomalyName = anomalyResponseObj.find(res => res.value === anomaly.feedback).name;
        setProperties(anomaly, {
          current: humanizeFloat(anomaly.current),
          baseline: humanizeFloat(anomaly.baseline),
          severity: parseFloat(anomaly.severity, 10).toFixed(2),
          anomalyFeedback: anomalyName
        });

        // Calculate change
        const changeFloat = (current - baseline) / baseline;
        setProperties(anomaly, {
          change: floatToPercent(changeFloat),
          humanizedChange: humanizeChange(changeFloat),
          duration: getFormatedDuration(anomaly.start, anomaly.end)
        });

        // Group anomalies by metricName and function name (alertName)
        //anomalyMapping[metricName][functionName].push(anomaly);TODO: not used now. Clean up to follow - lohuynh
        anomalyMapping[metricName].push(anomaly);
      });

      return anomalyMapping;
    } catch (error) {
      throw error;
    }
  }).drop(),

  /**
   * Retrieves metrics to index anomalies
   * @return {String[]} - array of strings, each of which is a metric
   * TODO: not used now. Clean up to follow - lohuynh
   */
  getMetrics() {
    let metricSet = new Set();
    this.get('applicationAnomalies').forEach(anomaly => {
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
    this.get('applicationAnomalies').forEach(anomaly => {
      alertSet.add(anomaly.functionName);
    });

    return [...alertSet];
  },

  /**
   * Sets the table column, metricList, and alertList
   * @return {undefined}
   */
  setupController(controller, model) {
    this._super(...arguments);
    controller.setProperties({
      columns,
      appNameSelected: model.applications.findBy('application', this.get('appName')),
      //metricList: this.getMetrics(),//TODO: clean up - lohuynh
      alertList: this.getAlerts(),
      appName: this.get('appName'),
      anomaliesCount:  Object.keys(model.anomalyMapping).length
    });
  }
});
