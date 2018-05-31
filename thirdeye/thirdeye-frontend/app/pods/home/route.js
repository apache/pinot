import Route from '@ember/routing/route';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';
import fetch from 'fetch';
import { hash } from 'rsvp';
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';
import { setProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import RSVP from "rsvp";
import moment from 'moment';
import { inject as service } from '@ember/service';
import _ from 'lodash';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Route.extend(AuthenticatedRouteMixin, {
  store: service('store'),
  anomaliesApiService: service('services/api/anomalies'),

  queryParams: {
    appName: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig
  },
  applicationAnomalies: null,
  appName: null,
  startDate: moment().subtract(1, 'day').utc().valueOf(), //taylored for Last 24 hours vs Today -> moment().startOf('day').utc().valueOf(),
  endDate: moment().utc().valueOf(),//taylored for Last 24 hours
  duration: '1d',//taylored for Last 24 hours

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
  async model(params) {
    const { appName, startDate, endDate, duration } = params;//check params
    const applications = await this.get('anomaliesApiService').queryApplications(appName, startDate);// Get all applicatons available

    return hash({
      appName,
      startDate,
      endDate,
      duration,
      applications
    });
  },

  afterModel(model) {
    // Overrides with params if exists
    const appName = model.appName || null;//model.applications.get('firstObject.application');
    const startDate = Number(model.startDate) || this.get('startDate');//TODO: we can use ember transform here
    const endDate = Number(model.endDate) || this.get('endDate');
    const duration = model.duration || this.get('duration');

    // Update props
    this.setProperties({
      appName,
      startDate,
      endDate,
      duration
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = appName ? await this.get('_getAnomalyMapping').perform(model) : [];//DEMO:
        const anomalyPerformance = appName ? await this.get('anomaliesApiService').queryPerformanceByAppNameUrl(appName, moment(this.get('startDate')).startOf('day').utc().format(), moment(this.get('endDate')).startOf('day').utc().format()) : [];
        const defaultParams = {
          anomalyMapping,
          anomalyPerformance,
          appName,
          startDate,
          endDate,
          duration
        };
        // Update model
        resolve(Object.assign(model, { ...defaultParams }));
      } catch (error) {
        reject(new Error(`Unable to retrieve anomaly data. ${error}`));
      }
    });
  },

  _getAnomalyMapping: task (function * (model) {//TODO: need to add to anomaly util - LH
    let anomalyMapping = {};
    //fetch the anomalies from the onion wrapper cache.
    const applicationAnomalies = yield this.get('anomaliesApiService').queryAnomaliesByAppName(this.get('appName'), this.get('startDate'), this.get('endDate'));
    const humanizedObject = {
      queryDuration: this.get('duration'),
      queryStart: this.get('startDate'),
      queryEnd: this.get('endDate')
    };
    this.set('applicationAnomalies', applicationAnomalies);

    applicationAnomalies.forEach(anomaly => {
      const metricName = anomaly.get('metricName');
      //Grouping the anomalies of the same metric name
      if (!anomalyMapping[metricName]) {
        anomalyMapping[metricName] = [];
      }

      // Group anomalies by metricName and function name (alertName) and wrap it into the Humanized cache. Each `anomaly` is the raw data from ember data cache.
      anomalyMapping[metricName].push(this.get('anomaliesApiService').getHumanizedEntity(anomaly, humanizedObject));
    });

    return anomalyMapping;
  }).drop(),

  /**
   * Retrieves metrics to index anomalies
   * @return {String[]} - array of strings, each of which is a metric
   * TODO: not used now. Clean up to follow - lohuynh
   */
  getMetrics() {
    let metricSet = new Set();
    this.get('applicationAnomalies').forEach(anomaly => {
      metricSet.add(anomaly.get('metric'));
    });
    return [...metricSet];
  },

  /**
   * Retrieves alerts to index anomalies
   * @return {String[]} - array of strings, each of which is a alerts
   */
  getAlerts() {
    let alertSet = new Set();
    const applicationAnomalies = this.get('applicationAnomalies');
    if (applicationAnomalies) {
      applicationAnomalies.forEach(anomaly => {
        alertSet.add(anomaly.get('functionName'));
      });
    }
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
      // alertList: this.getAlerts(),
      appName: this.get('appName'),
      anomaliesCount: this.get('applicationAnomalies.content') ? this.get('applicationAnomalies.content').length : 0
    });
  }
});
