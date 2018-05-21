import DS from 'ember-data';

export default DS.Model.extend({
  dataset: DS.attr(),
  metricName: DS.attr(),
  baselineTotal: DS.attr(),
  currentTotal: DS.attr(),
  globalRatio: DS.attr(),
  dimensions: DS.attr(),
  responseRows: DS.attr(),
  gainer: DS.attr(),
  loser: DS.attr(),
  dimensionCosts: DS.attr()
});
