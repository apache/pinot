import DS from 'ember-data';

export default DS.JSONSerializer.extend({
  /**
   * @summary A serializer hook. Normalizing the payload from api response simpler or legacy json that may not support the http://jsonapi.org/ spec
   * @method normalizeQueryResponse
   * @param {DS.Store} store
   * @param {DS.Model} primaryModelClass
   * @param {Object} payload - the data object
   * @return {Obect}
   */
  normalizeQueryResponse(store, primaryModelClass, payload) {
    // The response for this is just an object. So we need to normalize it into the meta.
    return {
      meta: payload,
      data: []
    };
  }
});
