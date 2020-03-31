import Route from '@ember/routing/route';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';
import { hash } from 'rsvp';
import { task } from 'ember-concurrency';
import RSVP from "rsvp";
import moment from 'moment';
import { inject as service } from '@ember/service';
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
    feedbackType: queryParamsConfig,
    subGroup: queryParamsConfig
  },
  anomalies: null,
  appName: null,
  startDate: moment().startOf('day').utc().valueOf(), //set default to 0:00 for data consistency
  endDate: moment().startOf('day').add(1, 'day').utc().valueOf(), //set default to 0:00 for data consistency
  duration: 'today', //set default to today
  feedbackType: 'All Resolutions',
  subGroup: null,

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
    const { appName, startDate, endDate, duration, feedbackType, subGroup } = params;//check params
    const applications = await this.get('anomaliesApiService').queryApplications();// Get all applicatons available
    const subscriptionGroups = await this.get('anomaliesApiService').querySubscriptionGroups(); // Get all subscription groups available
    const getAnomaliesTask = this.get('_getAnomalyMapping');

    return hash({
      appName,
      startDate,
      endDate,
      duration,
      applications,
      subscriptionGroups,
      feedbackType,
      subGroup,
      getAnomaliesTask
    });
  },

  afterModel(model) {
    // Overrides with params if exists
    let appName = model.appName || null;//model.applications.get('firstObject.application');
    let startDate = Number(model.startDate) || this.get('startDate');//TODO: we can use ember transform here
    let endDate = Number(model.endDate) || this.get('endDate');
    let duration = model.duration || this.get('duration');
    const feedbackType = model.feedbackType || this.get('feedbackType');
    const subGroup = model.subGroup || null;

    // if there are no selections made, set to tier0-tier1 application by default
    if (!appName && !subGroup) {
      appName = 'tier0-tier1';
      duration = '1w';
    }

    [startDate, endDate] = this.get('setDatesFromDuration')(duration, startDate, endDate); // if there's a duration param, override dates.

    // Update props
    this.setProperties({
      appName,
      startDate,
      endDate,
      duration,
      feedbackType,
      subGroup
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = (appName || subGroup) ? await this.get('_getAnomalyMapping').perform(model) : [];
        const alertsByMetric = (appName || subGroup) ? this.getAlertsByMetric() : [];
        const defaultParams = {
          anomalyMapping,
          appName,
          startDate,
          endDate,
          duration,
          feedbackType,
          alertsByMetric,
          subGroup
        };
        // Update model
        resolve(Object.assign(model, { ...defaultParams }));
      } catch (error) {
        reject(new Error(`Unable to retrieve anomaly data. ${error}`));
      }
    });
  },

  _getAnomalyMapping: task (function * () {//TODO: need to add to anomaly util - LH
    let anomalyMapping = {};
    let anomalies;
    //fetch the anomalies from the onion wrapper cache.
    if (this.get('appName') && this.get('subGroup')) {
      //this functionality is not provided in the UI, but the user can manually type the params into URL simultaneously
      anomalies = yield this.get('anomaliesApiService').queryAnomaliesByJoin(this.get('appName'), this.get('subGroup'), this.get('startDate'), this.get('endDate'));
    } else if (this.get('appName')) {
      anomalies = yield this.get('anomaliesApiService').queryAnomaliesByAppName(this.get('appName'), this.get('startDate'), this.get('endDate'));
    } else {
      anomalies = yield this.get('anomaliesApiService').queryAnomaliesBySubGroup(this.get('subGroup'), this.get('startDate'), this.get('endDate'));
    }
    const humanizedObject = {
      queryDuration: this.get('duration'),
      queryStart: this.get('startDate'),
      queryEnd: this.get('endDate')
    };
    this.set('anomalies', anomalies);

    anomalies.forEach(anomaly => {
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
    this.get('anomalies').forEach(anomaly => {
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
   * Overrides startDate and endDate params if duration present
   * @return {Undefined}
   */
  setDatesFromDuration(duration, start, end) {
    if (duration) {
      switch(duration) {
        case 'today':
          start = moment().startOf('day').valueOf();
          end = moment().startOf('day').add(1, 'days').valueOf();
          break;
        case '2d':
          start = moment().subtract(1, 'day').startOf('day').valueOf();
          end = moment().startOf('day').valueOf();
          break;
        case '1d':
          start = moment().subtract(24, 'hour').startOf('hour').valueOf();
          end = moment().startOf('hour').valueOf();
          break;
        case '1w':
          start = moment().subtract(1, 'week').startOf('day').valueOf();
          end = moment().startOf('day').add(1, 'days').valueOf();
          break;
      }
    }
    return [start, end];
  },

  /**
   * Sets the table column, metricList, and alertList
   * @return {undefined}
   */
  setupController(controller, model) {
    this._super(...arguments);
    const anomaliesBySelected = model.subGroup ? 'Subscription Group' : 'Application';
    controller.setProperties({
      columns,
      appNameSelected: model.applications.findBy('application', this.get('appName')),
      appName: this.get('appName'),
      anomaliesCount: this.get('anomalies.content') ? this.get('anomalies.content').length : 0,
      subGroupSelected: model.subscriptionGroups.findBy('name', this.get('subGroup')),
      subGroup: this.get('subGroup'),
      subscriptionGroups: model.subscriptionGroups,
      anomaliesBySelected
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
    },

    error() {
      return true;
    },

    /**
    * Refresh route's model.
    * @method refreshModel
    * @return {undefined}
    */
    refreshModel() {
      this.refresh();
    }
  }
});
