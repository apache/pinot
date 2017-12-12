import Ember from 'ember';
import moment from 'moment';
import { humanizeFloat } from 'thirdeye-frontend/helpers/utils'

export default Ember.Component.extend({
  entities: null, // {}

  anomalyUrn: null, // ""

  onFeedback: null, // func (urn, feedback, comment)

  isHiddenUser: null,

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
    function () {
      const { dataset, metric } =
        this.getProperties('dataset', 'metric');
      return `${dataset}::${metric}`;
    }
  ),

  dimensionsFormatted: Ember.computed('anomaly', function () {
    const dimensions = this.get('dimensions');
    return dimensions ? ` (${dimensions})` : '';
  }),

  changeFormatted: Ember.computed('change', function () {
    const change = this.get('change');
    const prefix = change > 0 ? '+' : '';
    return `${prefix}${humanizeFloat(change * 100)}%`;
  }),

  startFormatted: Ember.computed('anomaly', function () {
    return moment(this.get('anomaly').start).format('MMM D YYYY, hh:mm a');
  }),

  endFormatted: Ember.computed('anomaly', function () {
    return moment(this.get('anomaly').end).format('MMM D YYYY, hh:mm a');
  }),

  isNeedFeedback: Ember.computed('status', function () {
    return this.get('status') === 'NO_FEEDBACK';
  }),

  isHidden: Ember.computed(
    'isNeedFeedback',
    'isHiddenUser',
    function () {
      const { isNeedFeedback, isHiddenUser } = this.getProperties('isNeedFeedback', 'isHiddenUser');
      if (isHiddenUser === null) {
        return !isNeedFeedback;
      }
      return isHiddenUser;
    }
  ),

  actions: {
    onFeedback(status) {
      const { onFeedback, anomalyUrn } = this.getProperties('onFeedback', 'anomalyUrn');

      if (onFeedback) {
        onFeedback(anomalyUrn, status, '');
      }

      // TODO reload anomaly entity instead
      this.setProperties({ status });
    },

    toggleHidden() {
      const { isHidden } = this.getProperties('isHidden');
      this.setProperties({ isHiddenUser: !isHidden });
    }
  }
});
