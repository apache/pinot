import DS from 'ember-data';

export default DS.Model.extend({
  name: DS.attr(),
  active: DS.attr(),
  createdBy: DS.attr(),
  updatedBy: DS.attr(),
  properties: DS.attr(),
  yaml: DS.attr()
});
