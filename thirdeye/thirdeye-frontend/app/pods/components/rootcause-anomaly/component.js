import Ember from 'ember';
import moment from 'moment';

export default Ember.Component.extend({
  // TODO implement anomaly feedback

  entities: null, // {}

  anomalyUrn: null, // ""

  onFeedback: null, // func (urn, feedback, comment)

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
    return parseFloat(this.get('anomaly').attributes.current[0]);
  }),

  baseline: Ember.computed('anomaly', function () {
    return parseFloat(this.get('anomaly').attributes.baseline[0]);
  }),

  change: Ember.computed('anomaly', function () {
    const attr = this.get('anomaly').attributes;
    return parseFloat(attr.current[0]) / parseFloat(attr.baseline[0]) - 1;
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

  comment: Ember.computed('anomaly', function () {
    const attr = this.get('anomaly').attributes;
    if (attr.comment) {
      return attr.comment[0];
    } else {
      return [];
    }
  }),

  issueType: null, // TODO

  actions: {
    onFeedback(status) {
      const { onFeedback, anomalyUrn, comment } =
        this.getProperties('onFeedback', 'anomalyUrn','comment');

      if (onFeedback) {
        onFeedback(anomalyUrn, status, comment);
      }

      // TODO reload anomaly entity instead
      this.setProperties({ status });
    }
  }
});
