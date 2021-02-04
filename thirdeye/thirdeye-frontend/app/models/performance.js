import DS from 'ember-data';

/*
 * @description This model is unique as it does not match the query url.
 * @example /detection-job/eval/application/{appName}?
 */
export default DS.Model.extend({
  falseAlarm: DS.attr(),
  newTrend: DS.attr(),
  precision: DS.attr(),
  recall: DS.attr(),
  responseRate: DS.attr(),
  totalAlerts: DS.attr(),
  totalResponses: DS.attr(),
  trueAnomalies: DS.attr(),
  userReportAnomaly: DS.attr(),
  weightedPrecision: DS.attr()
});
