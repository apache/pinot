/**
 * Stats-Cards Component
 * Displays a row of cards, each of which contains stats, depending on the stats that are passed to the component
 * @module components/stats-card
 * @property {Object} stats  - [required] Stats object that specify values on each card.
 * @example
 * {{stats-cards
 *   stats=stats}}
 *
 * @exports stats-cards
 */
import Component from '@ember/component';
import { set, get, computed } from '@ember/object';

const STATS_INFO = {
  totalAnomalies: {
    title: 'Anomalies',
    description: 'Anomalies description',
    type: 'COUNT'
  },
  responseRate: {
    title: 'Response Rate',
    description: 'Response Rate description',
    type: 'PERCENT'
  },
  precision: {
    title: 'Precision',
    description: 'Precision description',
    type: 'PERCENT'
  },
  recall: {
    title: 'Recall',
    description: 'Recall description',
    type: 'PERCENT'
  }
};

export default Component.extend({
  classNames: ['te-horizontal-cards__container'],
  statsInfo: STATS_INFO,
  statsTransformed: null,
  areTwoSetsOfAnomalies: null, // passed in by parent

  oneCardOnly: computed('statsTransformed', function () {
    return this.get('statsTransformed').length < 2;
  }),

  /**
   * Transform the stats array passed to the component
   */
  didReceiveAttrs() {
    set(this, 'statsTransformed', this.statsBuilder(get(this, 'stats')));
  },

  /**
   * Given an array of values, configure the cards
   * @method statsBuilder
   * @param {Object} stats
   *   Information about different performance stats
   *   {
   *     totalAnomalies: {
   *       value: 2,
   *       type: "COUNT"
   *     },
   *     responseRate: {
   *       value: 10,
   *       type: "PERCENT"
   *     },
   *     precision: {
   *       value: 40,
   *       type: "PERCENT"
   *     },
   *     recall: {
   *       value: 30,
   *       type: "PERCENT"
   *    }
   *   }
   *
   * @return {Array<Object>}
   *   Each property of the object represents title, description and value for each stat card
   *
   */
  statsBuilder(stats = {}) {
    const statsTypes = Object.keys(this.statsInfo);

    const cards = statsTypes.reduce((memo, statsType) => {
      if (statsType in stats) {
        memo.push({ ...this.statsInfo[statsType], ...this.stats[statsType] });
      }

      return memo;
    }, []);

    return cards;
  }
});
