import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { assert } from '@ember/debug';
import EmberObject from '@ember/object';

/**
 * @type {Ember.Service}
 * @summary This service provides all the api calls for dimensions analysis data. An `Ember.Service`
   is a long-lived Ember object that can be made available in different parts of your application.
 * @example dimensionsApiService: service('services/api/dimensions');
 */
export default Service.extend({
  queryCache: service('services/query-cache'),

  init() {
    this._super();
    this._dimensionsCache = Object.create(null);
  },

  /**
   * @summary Fetch dimension breakdown for a given metric from the /dashboard/summary/autoDimensionOrder endpoint
   * @method queryDimensionsByMetric
   * @param {Object} queryOptions - all required query params for autoDimensionOrder class
   * @return {Ember.RSVP.Promise}
   * @example: for call dashboard/summary/autoDimensionOrder?dataset=search_v3_additive&metric=sat_click&currentStart=1524643200000&currentEnd=1525248000000&baselineStart=1524038400000&baselineEnd=1524643200000&summarySize=20&oneSideError=false&depth=3#
     usage: `this.get('dimensionApiService').queryDimensionsByMetric(dimensionParamsObj)`
   */
  async queryDimensionsByMetric(queryOptions) {

    const requiredKeys = [
      'metric',         // string: metric name
      'dataset',        // string: metric dataset
      'currentStart',   // number: analysis start ISO
      'currentEnd',     // number: analysis end ISO
      'baselineStart',  // number: baseline start ISO
      'baselineEnd',    // number: baseline end ISO
      'summarySize',    // number: number of results requested
      'depth',          // number: nesting levels 1 - 3
      'orderType'       // string: manual or auto
    ];

    requiredKeys.forEach((key) => {
      assert(`you must pass ${key} param as a required param`, queryOptions[key]);
    });

    const queryCache = this.get('queryCache');
    const dimensions = await queryCache.query('dimensions', queryOptions, { reload: false, cacheKey: queryCache.urlForQueryKey('dimensions', queryOptions) });
    return EmberObject.create(dimensions.meta);
  }

});
