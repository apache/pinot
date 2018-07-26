import DS from 'ember-data';

export default DS.Model.extend({
  application: DS.attr(),
  createdBy: DS.attr(),
  recipients: DS.attr(),
  updatedBy: DS.attr(),
  version: DS.attr()
});
