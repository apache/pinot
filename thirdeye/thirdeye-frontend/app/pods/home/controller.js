import Controller from '@ember/controller';
import { floatToPercent } from 'thirdeye-frontend/utils/utils';
import { computed, get } from '@ember/object';

export default Controller.extend({

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
        ['Number of anomalies', totalAlertsDescription, totalAlerts],
        ['Response Rate', responseRateDescription, floatToPercent(responseRate)],
        ['Precision', precisionDescription, floatToPercent(precision)],
        ['Recall', recallDescription, floatToPercent(recall)]
      ];

      return this.statsBuilder(statsArray);
    }
  ),

  /**
   * Given an array of values, configure the cards
   * @param {Array.Array<String>} - entries of each stats card (i.e. [['entry1', ...], ['entry2', ...], ['entry3], ...])
   * @return {Object[]} - array of objects, each of which represents a stats card
   * @example
   * [{
   *    title: 'title',
   *    description: 'description',
   *    value: 7
   *  }, {
   *    title: 'title',
   *    description: 'description',
   *    value: '87.1%'
   *  }, {
   *    title: 'title',
   *    description: 'description',
   *    value: '87.1%'
   * }];
   */
  statsBuilder(statsArray) {
    const props = ['title', 'description', 'value'];
    let cards = [];

    statsArray.forEach(card => {
      let obj = {};

      card.forEach((stat, index) => {
        const property = props[index];
        obj[property] = stat;
      });
      cards.push(obj);
    });

    return cards;
  }
});
