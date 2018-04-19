import DS from 'ember-data';

export default DS.Model.extend({
  start: DS.attr(),
  end: DS.attr(),
  dimensions: DS.attr(),
  severity: DS.attr(),
  current: DS.attr(),
  baseline: DS.attr(),
  feedback: DS.attr(),
  metricName: DS.attr(),
  metricId: DS.attr(),
  functionName: DS.attr(),
  functionId: DS.attr(),
  dataset: DS.attr()
});
