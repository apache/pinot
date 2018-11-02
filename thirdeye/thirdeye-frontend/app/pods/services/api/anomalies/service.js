import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { assert } from '@ember/debug';
import EmberObject, { computed, get } from '@ember/object';
import { humanizeFloat, humanizeChange } from 'thirdeye-frontend/utils/utils';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import {
  getFormatedDuration,
  anomalyResponseObj
} from 'thirdeye-frontend/utils/anomaly';

const HumanizedAnomaly = EmberObject.extend({// ex: record.humanizedChangeDisplay (humanized), record.anomaly.start (raw)
  id: computed.alias('anomaly.id'),
  _changeFloat: computed('anomaly.{current,baseline}', function() {
    const current = get(this, 'anomaly.current');
    const baseline = get(this, 'anomaly.baseline');
    return Number((current - baseline) / baseline);
  }),
  change: computed('_changeFloat', function() {
    return floatToPercent(get(this, '_changeFloat'));
  }),
  humanizedChangeDisplay: computed('_changeFloat', function() {
    return humanizeChange(get(this, '_changeFloat'));
  }),
  duration: computed('anomaly.{start,end}', function() {
    return getFormatedDuration(get(this, 'anomaly.start'), get(this, 'anomaly.end'));
  }),
  current: computed('anomaly.current', function() {
    return humanizeFloat(get(this, 'anomaly.current'));
  }),
  baseline: computed('anomaly.baseline', function() {
    return humanizeFloat(get(this, 'anomaly.baseline'));
  }),
  severity: computed('anomaly.severity', function() {
    return humanizeFloat(get(this, 'anomaly.severity'));
  }),
  source: computed('anomaly.source', function() {
    return humanizeFloat(get(this, 'anomaly.source'));
  }),
  anomalyFeedback: computed('anomaly.feedback', function() {
    return get(this, 'anomaly.feedback') ? anomalyResponseObj.find(res => res.value === get(this, 'anomaly.feedback')).name : '';
  }),
  queryDuration: computed('humanizedObject.queryDuration', function() {
    return get(this, 'humanizedObject.queryDuration');
  }),
  queryStart: computed('humanizedObject.queryStart', function() {
    return get(this, 'humanizedObject.queryStart');
  }),
  queryEnd: computed('humanizedObject.queryEnd', function() {
    return get(this, 'humanizedObject.queryEnd');
  }),
  classification: computed('anomaly.classification', function () {
    return get(this, 'anomaly.classification');
  })
});

/**
 * @type {Ember.Service}
 * @summary This service provides all the api calls for anomalies related data. An `Ember.Service`
   is a long-lived Ember object that can be made available in different parts of your application.
 * @example anomaliesApiService: service('services/api/anomalies');
 */
export default Service.extend({
  queryCache: service('services/query-cache'),

  init() {
    this._super();
    this._humanizedAnomaliesCache = Object.create(null);//create our humanized cache for this service (store humanized anomalies)
  },

  /**
   * @summary Return the cache for the humanized anomalies
   * @method getHumanizedAnomalies
   * @return {Object.array}
   * @example:
     usage: `this.get('anomaliesApiService').getHumanizedAnomalies();`
   */
  async getHumanizedAnomalies() {
    return this._humanizedAnomaliesCache;
  },

  /**
   * @summary Return the cached humanized anomaly if exists, if not we store it into cache and return it.
     1. Check for the existance of the entity (anomaly record) in the cache by cacheKey/id.
     2. Add the entity (anomaly record) to the `HumanizedAnomaly` to be used later. This save us the need to directly access the store (find/findRecord).
     3. Save to the cache the new HumanizedAnomaly ember object to cache. This contains all the display/humanized properties, including the anomaly record itself.
        `HumanizedAnomaly.create({ entity })`
     4. Assign to humanizedEntity the new HumanizedAnomaly ember object. This allow us not to mutate the actual anomaly record later.
     5. Return the humanizedEntity - We now can access the raw data or humanized data
   * @method getHumanizedEntity
   * @param {object} anomaly - a raw anomaly record from the store cache (proxy model). This param name must match the name used in `HumanizedAnomaly`
   * @param {object} humanizedEntity - The object that contains any additional humanized items.
   * @return {Ember.Object}
   * @example:
      const record = this.get('anomaliesApiService').getHumanizedEntity(entity);
      const humanizedSeverity = record.severity;
      const rawSeverity = record.anomaly.severity;
   */
  getHumanizedEntity(anomaly, humanizedObject) {
    assert('you must pass anomaly record.', anomaly);

    let cacheKey = get(anomaly, 'id');
    let humanizedEntity = this._humanizedAnomaliesCache[cacheKey];//retrieve the anomaly from cache if exists
    if (!humanizedEntity) {
      humanizedEntity = this._humanizedAnomaliesCache[cacheKey] = HumanizedAnomaly.create({ anomaly, humanizedObject });// add to our dictionary
    }

    return humanizedEntity;
  },

  /**
   * @summary Fetch all application names. We can use it to list in the select box.
   * @method queryApplications
   * @param {String} appName - the application name for creating the cacheKey
   * @return {Ember.RSVP.Promise}
   * @example: /thirdeye/entity/APPLICATION
     usage: `this.get('anomaliesApiService').queryApplications();`
   */
  async queryApplications(appName, start) {
    const queryCache = this.get('queryCache');
    const modelName = 'application';
    const query = { appName, start };//remove end to persist cache
    const cacheKey = queryCache.urlForQueryKey(modelName, {});//TODO: Won't pass all the `query` here. The `cacheKey` do not need to be uniqued, since all apps has the same list of apps.
    const applications = await queryCache.query(modelName, query, { reload: false, cacheKey });
    return applications;
  },

  /**
   * @summary Fetch all anomalies by application name and start time
   * @method queryAnomaliesByAppName
   * @param {String} appName - the application name
   * @param {Number} startStamp - the anomaly iso start time
   * @return {Ember.RSVP.Promise}
   * @example: for call `/userdashboard/anomalies?application={someAppName}&start={1508472800000}`
     usage: `this.get('anomaliesApiService').queryAnomaliesByAppName(this.get('appName'), this.get('startDate'));`
   */
  async queryAnomaliesByAppName(appName, start, end) {
    assert('you must pass appName param as an required argument.', appName);
    assert('you must pass start param as an required argument.', start);

    const queryCache = this.get('queryCache');
    const modelName = 'anomalies';
    const query = { application: appName, start, end };
    const anomalies = await queryCache.query(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
    return anomalies;
  }
});
