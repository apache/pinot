import Ember from 'ember';

// TODO: save this in a constant file
const GRANULARITY_MAPPING = {
  DAYS: 'M/D',
  HOURS: 'M/D hh',
  MINUTES: 'M/D hh:mm a'
};

export default Ember.Component.extend({
  metrics: null,
  showDetails: false,
  granularity: 'DAYS',
  primaryMetric: null,
  relatedMetrics: null,

  /**
   * Determines the date format based on granularity
   */
  dateFormat: Ember.computed('granularity', function() {
    const granularity = this.get('granularity');
    return GRANULARITY_MAPPING[granularity]
  }),

  /**
   * Derives dates from the primary metric
   */
  dates: Ember.computed.reads('primaryMetric.timeBucketsCurrent'),
  
  primaryMetricRows: Ember.computed('primaryMetric', function() {
    const metrics = this.get('primaryMetric');

    return Ember.isArray(metrics) ? metrics : [metrics];
  }),

  relatedMetricRows: Ember.computed('relatedMetrics', function() {
    const metrics = this.get('relatedMetrics');
    
    return Ember.isArray(metrics) ? metrics : [metrics];
  })
});
