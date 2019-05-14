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
  setProperties
} from '@ember/object';
import { appendFilters } from 'thirdeye-frontend/utils/rca-utils';
import { humanizeFloat, humanizeChange, checkStatus } from 'thirdeye-frontend/utils/utils';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';

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
    shareId: queryParamsConfig,
    subGroup: queryParamsConfig
  },
  appName: null,
  startDate: moment().subtract(1, 'day').utc().valueOf(), //taylored for Last 24 hours vs Today -> moment().startOf('day').utc().valueOf(),
  endDate: moment().utc().valueOf(), //Last 24 hours
  duration: '1d', //Last 24 hours
  feedbackType: 'All Resolutions',
  shareId: null,
  subGroup: null,

  async model(params) {
    const { appName, startDate, endDate, duration, feedbackType, shareId, subGroup } = params;//check params
    const applications = await get(this, 'anomaliesApiService').queryApplications();// Get all applicatons available
    const subscriptionGroups = await this.get('anomaliesApiService').querySubscriptionGroups(); // Get all subscription groups available

    return hash({
      appName,
      startDate,
      endDate,
      duration,
      applications,
      feedbackType,
      shareId,
      subscriptionGroups,
      subGroup
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
    const subGroup = model.subGroup || null;
    // Update props
    setProperties(this, {
      appName,
      startDate,
      endDate,
      duration,
      feedbackType,
      subGroup
    });

    return new RSVP.Promise(async (resolve, reject) => {
      try {
        const anomalyMapping = (appName || subGroup) ? await get(this, '_getAnomalyMapping').perform(model) : []; //DEMO:
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
      queryDuration: get(this, 'duration'),
      queryStart: get(this, 'startDate'),
      queryEnd: get(this, 'endDate')
    };
    set(this, 'anomalies', anomalies);
    let index = 1;

    anomalies.forEach(anomaly => {
      const metricName = get(anomaly, 'metricName');
      const metricId = get(anomaly, 'metricId');
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

    anomalyMapping = yield get(this, '_fetchOffsets').perform(anomalyMapping);
    return anomalyMapping;
  }).drop(),

  _fetchOffsets: task (function * (anomalyMapping) {
    if (!anomalyMapping) { return; }

    let map = {};
    let index = 1;
    // Iterate through each anomaly
    yield Object.keys(anomalyMapping).some(function(metric) {
      Object.keys(anomalyMapping[metric].items).some(function(alert) {
        anomalyMapping[metric].items[alert].items.forEach(async (item) => {

          const anomaly = item.anomaly;
          const metricName = get(anomaly, 'metricName');
          const metricId = get(anomaly, 'metricId');
          const functionName = get(anomaly, 'functionName');
          const functionId = get(anomaly, 'functionId');

          const dimensions = get(anomaly, 'dimensions');
          const start = get(anomaly, 'start');
          const end = get(anomaly, 'end');

          if (!map[metricName]) {
            map[metricName] = { 'metricId': metricId, items: {}, count: index };
            index++;
          }

          if(!map[metricName].items[functionName]) {
            map[metricName].items[functionName] = { 'functionId': functionId, items: [] };
          }

          const filteredDimensions = Object.keys(dimensions).map(key => [key, '=', dimensions[key]]);
          //build new urn
          const metricUrn = appendFilters(`thirdeye:metric:${metricId}`, filteredDimensions);
          //Get all in the following order - current,wo2w,median4w
          const offsets = await fetch(`/rootcause/metric/aggregate/batch?urn=${metricUrn}&start=${start}&end=${end}&offsets=wo1w,wo2w,median4w&timezone=America/Los_Angeles`).then(checkStatus).then(res => res);

          const current = get(anomaly, 'current');
          const wow = humanizeFloat(offsets[0]);
          const wo2w = humanizeFloat(offsets[1]);
          const median4w = humanizeFloat(offsets[2]);
          const wowChange = floatToPercent(Number((current - offsets[0]) / offsets[0]));
          const wo2wChange = floatToPercent(Number((current - offsets[1]) / offsets[1]));
          const median4wChange = floatToPercent(Number((current - offsets[2]) / offsets[2]));
          const wowHumanizeChange = humanizeChange(Number((current - offsets[0]) / offsets[0]));
          const wo2wHumanizeChange = humanizeChange(Number((current - offsets[1]) / offsets[1]));
          const median4wHumanizeChange = humanizeChange(Number((current - offsets[2]) / offsets[2]));

          set(anomaly, 'offsets',  offsets ? {
            'wow': { value: wow, change: wowChange, humanizedChangeDisplay: wowHumanizeChange },
            'wo2w': { value: wo2w, change: wo2wChange, humanizedChangeDisplay: wo2wHumanizeChange },
            'median4w': { value: median4w, change: median4wChange, humanizedChangeDisplay: median4wHumanizeChange }
          } : {
            'wow': '-',
            'wo2w': '-',
            'median4w': '-'
          });

          map[metricName].items[functionName].items.push(item);
        });
      });
    });
    // return updated anomalyMapping
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
          'shareUrl': null
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
      subGroupDisplay: get(this, 'subGroup'),
      anomaliesCount: get(this, 'anomalies.content') ? get(this, 'anomalies.content').length : 0
    });
  }
});
