/**
 * Stats-Cards Component
 * Displays a row of cards, each of which contains stats, depending on the stats that are passed to the component
 * @module components/anomaly-stats-block
 * @property {object[]} stats  - [required] array of stats object that specify what to display on each card
 *
 * @example
 * {{stats-cards
 *   stats=stats}}
 *
 * @exports stats-cards
 */
import Component from '@ember/component';
import { set, get } from '@ember/object';

export default Component.extend({
  classNames: ['te-horizontal-cards__container'],

  /**
   * Transform the stats array passed to the component
   */
  didReceiveAttrs() {
    set(this,
      'stats',
      this.statsBuilder(get(this, 'stats')));
  },

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
