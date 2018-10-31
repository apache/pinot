import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { hash } from 'rsvp';
import { task } from 'ember-concurrency';
import RSVP from "rsvp";
import moment from 'moment';
import { inject as service } from '@ember/service';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumnsShared';
import {
  get,
  set,
  computed,
  setProperties
} from '@ember/object';

const queryParamsConfig = {
  refreshModel: true
};

export default Route.extend(AuthenticatedRouteMixin, {
  anomaliesApiService: service('services/api/anomalies'),
  shareDashboardApiService: service('services/api/share-dashboard'),
  shareTemplateConfigApiService: service('services/api/share-template-config'),
  session: service(),
  queryParams: {
    appName: queryParamsConfig,
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    duration: queryParamsConfig,
    feedbackType: queryParamsConfig,
    shareId: queryParamsConfig
  },
  appName: null,
  startDate: moment().subtract(1, 'day').utc().valueOf(), //taylored for Last 24 hours vs Today -> moment().startOf('day').utc().valueOf(),
  endDate: moment().utc().valueOf(),//Last 24 hours
  duration: '1d',//Last 24 hours
  feedbackType: 'All Resolutions',
  shareId: null,

  async model(params) {
    const { appName, startDate, endDate, duration, feedbackType, shareId } = params;//check params
    const applications = await get(this, 'anomaliesApiService').queryApplications(appName, startDate);// Get all applicatons available

    return hash({
      appName,
      startDate,
      endDate,
      duration,
      applications,
      feedbackType,
      shareId
    });
  },

  afterModel(model) {
    // Overrides with params if exists
    const appName = model.appName || null;
    const startDate = Number(model.startDate) || get(this, 'startDate');//TODO: we can use ember transform here
    const endDate = Number(model.endDate) || get(this, 'endDate');
    const duration = model.duration || get(this, 'duration');
    const feedbackType = model.feedbackType || get(this, 'feedbackType');
    const shareId = model.shareId || get(this, 'shareId');
    // Update props
    setProperties(this, {
      appName,
      startDate,
      endDate,
      duration,
      feedbackType
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = appName ? await get(this, '_getAnomalyMapping').perform(model) : [];//DEMO:
        const shareMetaData = shareId ? await get(this, 'shareDashboardApiService').queryShareMetaById(shareId) : [];
        const shareTemplateConfig = appName ? await get(this, 'shareTemplateConfigApiService').queryShareTemplateConfigByAppName(appName) : {};
        const defaultParams = {
          anomalyMapping,
          shareMetaData,
          shareTemplateConfig,
          appName,
          startDate,
          endDate,
          duration,
          feedbackType,
          shareId
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
    //fetch the anomalies from the onion wrapper cache.
    const applicationAnomalies = yield get(this, 'anomaliesApiService').queryAnomaliesByAppName(get(this, 'appName'), get(this, 'startDate'), get(this, 'endDate'));
    const humanizedObject = {
      queryDuration: get(this, 'duration'),
      queryStart: get(this, 'startDate'),
      queryEnd: get(this, 'endDate')
    };
    set(this, 'applicationAnomalies', applicationAnomalies);
    let index = 1;

    applicationAnomalies.forEach(anomaly => {
      const metricName = get(anomaly, 'metricName');
      const metricId = get(anomaly, 'metricId');
      const id = get(anomaly, 'id');
      const functionName = get(anomaly, 'functionName');
      const functionId = get(anomaly, 'functionId');
      //Grouping the anomalies of the same metric name
      if (!anomalyMapping[metricName]) {
        anomalyMapping[metricName] = { 'metricId': metricId, items: {}, count: index };
        index++;
      }
      //By Alert first time
      if(!anomalyMapping[metricName].items[functionName]) {
        anomalyMapping[metricName].items[functionName] = { 'functionId': functionId, items: [] };
      }

      // Group anomalies by metricName and function name (alertName) and wrap it into the Humanized cache. Each `anomaly` is the raw data from ember data cache.
      anomalyMapping[metricName].items[functionName].items.push(get(this, 'anomaliesApiService').getHumanizedEntity(anomaly, humanizedObject));
    });

    return anomalyMapping;
  }).drop(),

  actions: {
    willTransition: function(transition){
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }

      if (transition.targetName !== 'home.share-dashboard') {
        //reset on leaving this route only vs calling itself
        this.controller.setProperties({
          'shareId': null,
          'showTooltip': false,
          'shareUrl': null,
        });
      }
    }
  },

  /**
   * Sets the table column, metricList, and alertList to controller/view
   * @return {undefined}
   */
  setupController(controller, model) {
    this._super(...arguments);
    //set and reset controller props as needed
    controller.setProperties({
      shareTemplateConfig: model.shareTemplateConfig.data || {},
      columns,
      start: get(this, 'startDate'),
      end: get(this, 'endDate'),
      startDateDisplay:  moment(get(this, 'startDate')).format('MM/DD/YYYY'),
      endDateDisplay: moment(get(this, 'endDate')).format('MM/DD/YYYY'),
      appNameDisplay: get(this, 'appName'),
      anomaliesCount: get(this, 'applicationAnomalies.content') ? get(this, 'applicationAnomalies.content').length : 0
    });
  }
});
