import DS from 'ember-data';
import Ember from 'ember';

export default DS.JSONSerializer.extend({
  normalizeResponse(store, primaryModelClass, payload /** ,id, requestType*/) {
    //we are kind of doing the job of the this._super(...) here to convert a 'classic JSON' payload into JSON API.
    let data = {};
    let attributes = payload;

    data.id = payload.id || Ember.generateGuid();
    data.type = primaryModelClass.modelName;
    data.attributes = attributes;

    return {
      data
    };
  }
});
