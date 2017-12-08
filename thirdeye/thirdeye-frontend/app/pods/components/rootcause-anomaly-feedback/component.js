import Ember from 'ember';
import moment from 'moment';

export default Ember.Component.extend({
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

  status: Ember.computed('anomaly', function () {
    return this.get('anomaly').attributes.status[0];
  }),

  actions: {
    onFeedback(status) {
      const { onFeedback, anomalyUrn } = this.getProperties('onFeedback', 'anomalyUrn');

      if (onFeedback) {
        onFeedback(anomalyUrn, status, '');
      }

      // TODO reload anomaly entity instead
      this.setProperties({ status });
    }
  }
});
