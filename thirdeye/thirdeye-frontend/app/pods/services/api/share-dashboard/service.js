import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { assert } from '@ember/debug';
import CryptoJS from 'cryptojs';

/**
 * @type {Ember.Service}
 * @summary This service provides all the api calls for storing or fetching share dashboard data. An `Ember.Service`
   is a long-lived Ember object that can be made available in different parts of your application.
 * @example shareDashboardApiService: service('services/api/shareDashboard');
 */
export default Service.extend({
  queryCache: service('services/query-cache'),
  store: service('store'),
  hashKey: null,
  init() {
    this._super();
    //this._dimensionsCache = Object.create(null);
  },

  getHashKey() {
    return this.get('hashKey');
  },

  /**
   * @summary Fetch all share dashboard meta data (json) by athe hashkey provided
   * @method queryShareMetaById
   * @param {String} id - the shareId/hashKey
   * @return {Ember.RSVP.Promise}
   * @example: for call /config/shareDashboard/${shareId}
     usage: `this.get('shareDashboardApiService').queryShareMetaById(${shareId});`
   */
  async queryShareMetaById(shareId) {
    assert('You must pass shareId param as an required argument.', shareId);

    const queryCache = this.get('queryCache');
    //share is a modelName (must match your model's name)
    const modelName = 'share';
    const query = { shareId };
    const shareMetaData = await queryCache.query(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
    return shareMetaData;
  },

  /**
   * @summary Save the share dashboard meta data (json)
   * @method saveShareDashboard
   * @param {Object} json - the json data
   * @return {Ember.RSVP.Promise}
   * @example: `this.get('shareDashboardApiService').saveShareDashboard({json})`
   */
  async saveShareDashboard(data) {
    assert('You must pass data param as an required argument.', data);

    const blob = { id: 0, blob: data };//dummy id for now. ember data requires an id on the client side set here or in serializer else an erroris thrown.
    const hashKey = CryptoJS.SHA256(JSON.stringify(blob)).toString();//TODO: store in a service object - lohuynh
    blob.id = hashKey;
    this.set('hashKey', hashKey);

    //share is a modelName (must match your model's name)
    let record =  this.get('store').createRecord('share', blob);
    //record is a DS.Model
    let promise = await record.save();

    return promise;
  }
});
