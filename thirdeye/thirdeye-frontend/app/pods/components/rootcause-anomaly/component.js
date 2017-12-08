import Ember from 'ember';
import moment from 'moment';
import { humanizeFloat } from 'thirdeye-frontend/helpers/utils'

export default Ember.Component.extend({
  // TODO implement anomaly feedback

  entities: null, // {}

  anomalyUrn: null, // ""

  onFeedback: null, // func (urn, feedback, comment)

  /**
   * Options to populate anomaly dropdown
   */
  options: [
    'ANOMALY',
    'ANOMALY_NEW_TREND',
    'NOT_ANOMALY',
    'NO_FEEDBACK'
  ],

  anomaly: Ember.computed(
    'entities',
    'anomalyUrn',
    function () {
      const { entities, anomalyUrn } = this.getProperties('entities', 'anomalyUrn');

      if (!anomalyUrn || !entities || !entities[anomalyUrn]) { return false; }

      return entities[anomalyUrn];
    }
  ),

  functionName: Ember.computed('anomaly', function () {
    return this.get('anomaly').attributes.function[0];
  }),

  anomalyId: Ember.computed('anomaly', function () {
    return this.get('anomaly').urn.split(':')[3];
  }),

  metric: Ember.computed('anomaly', function () {
    return this.get('anomaly').attributes.metric[0];
  }),

  dataset: Ember.computed('anomaly', function () {
    return this.get('anomaly').attributes.dataset[0];
  }),

  current: Ember.computed('anomaly', function () {
    return parseFloat(this.get('anomaly').attributes.current[0]).toFixed(3);
  }),

  baseline: Ember.computed('anomaly', function () {
    return parseFloat(this.get('anomaly').attributes.baseline[0]).toFixed(3);
  }),

  change: Ember.computed('anomaly', function () {
    const attr = this.get('anomaly').attributes;
    return (parseFloat(attr.current[0]) / parseFloat(attr.baseline[0]) - 1);
  }),

  status: Ember.computed('anomaly', function () {
    return this.get('anomaly').attributes.status[0];
  }),

  duration: Ember.computed('anomaly', function () {
    const anomaly = this.get('anomaly');
    return moment.duration(anomaly.end - anomaly.start).humanize();
  }),

  dimensions: Ember.computed('anomaly', function () {
    const attr = this.get('anomaly').attributes;
    const dimNames = attr.dimensions;
    const dimValues = dimNames.reduce((agg, dimName) => { agg[dimName] = attr[dimName][0]; return agg; }, {});
    return dimNames.sort().map(dimName => dimValues[dimName]).join(', ');
  }),

  issueType: null, // TODO

  metricFormatted: Ember.computed(
    'dataset',
    'metric',
    'dimensions',
    function () {
      const { dataset, metric, dimensions } =
        this.getProperties('dataset', 'metric', 'dimensions');
      const dimensionsString = dimensions ? ` (${dimensions})` : '';
      return `${dataset}::${metric}${dimensionsString}`
    }
  ),

  changeFormatted: Ember.computed('change', function () {
    const change = this.get('change');
    const prefix = change > 0 ? '+' : '';
    return `${prefix}${humanizeFloat(change * 100)}%`;
  })

});
