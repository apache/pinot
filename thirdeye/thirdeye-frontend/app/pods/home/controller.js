import Controller from '@ember/controller';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import { computed, get, set } from '@ember/object';

export default Controller.extend({

  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table-bordered table-condensed te-anomaly-table--no-margin'
  },

  /**
   * Stats to display in cards
   * @type {Object[]} - array of objects, each of which represents a stats card
   */
  stats: computed(
    'model.anomalyPerformance',
    function() {
      const { totalAlerts, responseRate, precision, recall } = get(this, 'model.anomalyPerformance');
      const totalAlertsDescription = 'Total number of anomalies that occured over a period of time';
      const responseRateDescription = '% of anomalies that are reviewed';
      const precisionDescription = '% of all anomalies detected by the system that are true';
      const recallDescription = '% of all anomalies detected by the system';
      const statsArray = [
        ['Number of anomalies', totalAlertsDescription, totalAlerts, 'digit'],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate), 'percent'],
        ['Precision', precisionDescription, floatToPercent(precision), 'percent'],
        ['Recall', recallDescription, floatToPercent(recall), 'percent']
      ];

      return statsArray;
    }
  ),

  actions: {
    /**
     * Sets the selected application property based on user selection
     * @param {Object} selectedApplication - object that represents selected application
     * @return {undefined}
     */
    selectApplication(selectedApplication) {
      set(this, 'defaultApplication', selectedApplication);
    }
  }
});
