import DS from 'ember-data';
/*
 * @description This model is unique as it does not match the query url.
 * @example /config/shareDashboard/{mykey}
 */
export default DS.Model.extend({
  blob: DS.attr()
});
