import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { assert } from '@ember/debug';

// set expiration to 1 day
// const EXPIRATION = 60 * 60 * 24 * 1;

/**
 * @type {Ember.Service}
 * @summary This service is to also cached the query response data for `this.get('store').query()` calls. In general,
   the call to `this.get('store').query()` does not do request caching under the hood.
   The `onion wrapper` cache only caches the info that ember-data does not currently cache for us. Meaning if the internal records
   in the ember store changes it changes for this cache as well.
   This is caching the `onion wrapper` we get back with the data document.
   An `Ember.Service` is a long-lived Ember object that can be made available in different parts of your application.
 * @example  a typical json-api doc.
    {
      links: {}
      meta: {},
      data: [] | {}  <- Note: this will have the entity or entities that the ember-data will cache in its store.
    }
 * @see {@link http://jsonapi.org/|jsonapi.org}
 * //DEMO: To debug what is in the store - this.get('store')._identityMap.
 * //assets->addon-tree-output->modules/ember-data->_private.js (_push: function _push(jsonApiDoc)
 * @example   anomaliesApiService: service('services/api/anomalies'),
 */
export default Service.extend({
  init() {
    this._super(...arguments);
    this._cache = Object.create(null);
  },

  store: service('store'),

  /**
   * @summary  This query method is a wrapper of the ember store `query`. It will add the dictionary caching that we need.
   * @method query
   * @param {String} type - modelName
   * @param {Object} query - query object used to generate the
   * @param {Object} adapterOptions - adapter options object (reload: true will enforces getting fresh data)
   * @example
       const query = { application: appName, start };
       const anomalies = await queryCache.query(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
   * @return {Object}
   */
  async query(type, query, adapterOptions) {
    const cacheKey = adapterOptions.cacheKey;
    assert('you must pass adapterOptions to queryCache.query', adapterOptions);
    assert('you must include adapterOptions.cacheKey', cacheKey);

    let cached = this._cache[cacheKey];
    if (!cached || adapterOptions.reload) { // TODO: Add `EXPIRATION` here to purge when possible - lohuynh
      cached = this._cache[cacheKey] = await this.get('store').query(type, query, adapterOptions);
    }
    return cached;
  },

  /**
   * @summary  This queryRecord method is a wrapper of the ember store `queryRecord`. It will add the dictionary caching that we need.
   * @method query
   * @param {String} type - modelName
   * @param {Object} query - query object used to generate the
   * @param {Object} adapterOptions - adapter options object (reload: true will enforces getting fresh data)
   * @example
       const query = { application: appName, start };
       const anomalies = await queryCache.queryRecord(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
   * @return {Object}
   */
  async queryRecord(type, query, adapterOptions) {
    const cacheKey = adapterOptions.cacheKey;
    assert('you must pass adapterOptions to queryCache.query', adapterOptions);
    assert('you must include adapterOptions.cacheKey', cacheKey);

    let cached = this._cache[cacheKey];
    if (!cached || adapterOptions.reload) {// TODO: Add `EXPIRATION` here to purge when possible - lohuynh
      cached = this._cache[cacheKey] = await this.get('store').queryRecord(type, query, adapterOptions);
    }
    return cached;
  },

  /**
   * @summary  We can use the request query object as the `cacheKey` in the `query` hook.
     The cacheKey should be uniqued, which would be different with each change in query request params (Note: this is not the url query).
   * @method urlForQueryKey
   * @param {String} modelName
   * @param {Object} query
   * @example
       const query = { application: appName, start };
       const anomalies = await queryCache.query(modelName, query, { reload: false, cacheKey: queryCache.urlForQueryKey(modelName, query) });
   * @return {String}
   */
  urlForQueryKey(modelName, query) {
    const key = `${this.get('store').adapterFor(modelName).urlForQuery(query, modelName)}::${JSON.stringify(query)}`;//making a unique `cacheKey`. + JSON.stringify(query)
    return key;
  }
});
