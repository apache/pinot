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
  anomaliesApiService: service('services/api/anomalies'),
  session: service(),

  queryParams: {
    appName: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    duration: queryParamsConfig,
    feedbackType: queryParamsConfig
  },
  applicationAnomalies: null,
  appName: null,
  startDate: moment().subtract(1, 'day').utc().valueOf(), //taylored for Last 24 hours vs Today -> moment().startOf('day').utc().valueOf(),
  endDate: moment().utc().valueOf(),//taylored for Last 24 hours
  duration: '1d',//taylored for Last 24 hours
  feedbackType: 'All Resolutions',

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
    const { appName, startDate, endDate, duration, feedbackType } = params;//check params
    const applications = await this.get('anomaliesApiService').queryApplications(appName, startDate);// Get all applicatons available

    return hash({
      appName,
      startDate,
      endDate,
      duration,
      applications,
      feedbackType
    });
  },

  afterModel(model) {
    // Overrides with params if exists
    const appName = model.appName || null;//model.applications.get('firstObject.application');
    const startDate = Number(model.startDate) || this.get('startDate');//TODO: we can use ember transform here
    const endDate = Number(model.endDate) || this.get('endDate');
    const duration = model.duration || this.get('duration');
    const feedbackType = model.feedbackType || this.get('feedbackType');
    // Update props
    this.setProperties({
      appName,
      startDate,
      endDate,
      duration,
      feedbackType
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = appName ? await this.get('_getAnomalyMapping').perform(model) : [];//DEMO:
        const alertsByMetric = appName ? this.getAlertsByMetric() : [];
        const defaultParams = {
          anomalyMapping,
          appName,
          startDate,
          endDate,
          duration,
          feedbackType,
          alertsByMetric
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
   * Retrieves alerts based on metric name
   * @return {Object} - associative array of alerts by metric
   */
  getAlertsByMetric() {
    const metricsObj = {};
    this.get('applicationAnomalies').forEach(anomaly => {
      let functionName = anomaly.get('functionName');
      let functionId = anomaly.get('functionId');
      let metricName = anomaly.get('metricName');
      if (!metricsObj[metricName]) {
        metricsObj[metricName] = { names: [], selectedIndex: 0, ids: [] };
      }
      //let alertIncluded = metricsObj[metricName].find(alert => { alert.id === functionId });
      if (metricsObj[metricName] && !metricsObj[metricName].names.includes(functionName)) {
        metricsObj[metricName].names.push(functionName);
        metricsObj[metricName].ids.push(functionId);
      }
    });
    return metricsObj;
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
      appName: this.get('appName'),
      anomaliesCount: this.get('applicationAnomalies.content') ? this.get('applicationAnomalies.content').length : 0
    });
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    }
  }
});
