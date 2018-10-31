import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { assert } from '@ember/debug';

/**
 * @type {Ember.Service}
 * @summary This service provides all the api calls for storing or fetching share template config data. An `Ember.Service`
   is a long-lived Ember object that can be made available in different parts of your application.
 * @example shareTemplateConfigApiService: service('services/api/shareTemplateConfig');
 */
export default Service.extend({
  queryCache: service('services/query-cache'),
  store: service('store'),
  hashKey: null,
  init() {
    this._super();
  },

  getHashKey() {
    return this.get('hashKey');
  },

  /**
   * @summary Fetch all share dashboard custom template meta (config) by app name
   * @method queryShareTemplateConfigByAppName
   * @param {String} appName - the application name
   * @return {Ember.RSVP.Promise}
   * @example: for fetch on `/config/shareDashboardTemplates/${appName}`
     usage: this.get('shareTemplateConfigApiService').queryShareTemplateConfigByAppName(${appName});
   */
  async queryShareTemplateConfigByAppName(appName) {
    assert('You must pass appName param as an required argument.', appName);

    const queryCache = this.get('queryCache');
    //share-config is a modelName (must match your model's name)
    const modelName = 'share-config';
    const query = { appName };
    let shareMetaData = {};
    try{
      shareMetaData = await queryCache.queryRecord(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
    } catch (error) {
      shareMetaData = {};
    }
    return shareMetaData;
  }
});
