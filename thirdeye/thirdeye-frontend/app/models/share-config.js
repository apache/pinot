import DS from 'ember-data';
/*
 * @description This model is unique as the query is dynamic base on appName.
 * @example ${this.get('namespace')}/${query.appName};
 */
export default DS.Model.extend({
  blob: DS.attr()
});
