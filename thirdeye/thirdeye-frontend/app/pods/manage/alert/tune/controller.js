/**
 * Controller for Alert Details Page: Tune Sensitivity Tab
 * @module manage/alert/tune
 * @exports manage/alert/tune
 */
import Controller from '@ember/controller';

export default Controller.extend({

  /**
   * Data needed to render the stats 'cards' above the anomaly graph for this alert
   * TODO: this will become a component
   * @type {Object}
   */
  anomalyStats: Ember.computed(
    'totalAnomalies',
    function() {
      const total = this.get('totalAnomalies') || 0;
      const anomalyStats = [
        {
          title: 'Number of anomalies',
          text: 'Estimated average number of anomalies per month',
          value: total,
          projected: '5'
        },
        {
          title: 'Response rate',
          text: 'Percentage of anomalies that has a response',
          value: '87.1%'
        },
        {
          title: 'Precision',
          text: 'Among all anomalies detected, the percentage of them that are true.',
          value: '50%',
          projected: '89.2%'
        },
        {
          title: 'Recall',
          text: 'Among all anomalies that happened, the percentage of them detected by the system',
          value: '25%',
          projected: '89.2%'
        },
        {
          title: 'MTTD for >30% change',
          text: 'Minimum time to detect for anomalies with > 30% change',
          value: '4.8 mins',
          projected: '5 mins'
        }
      ];
      return anomalyStats;
    }
  )

});
