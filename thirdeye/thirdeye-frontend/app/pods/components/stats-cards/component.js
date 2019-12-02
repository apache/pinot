/**
 * Stats-Cards Component
 * Displays a row of cards, each of which contains stats, depending on the stats that are passed to the component
 * @module components/stats-card
 * @property {object[]} stats  - [required] array of stats object that specify values on each card. This will be
 *                               transformed inside the component
 * @example
 * {{stats-cards
 *   stats=stats}}
 *
 * @exports stats-cards
 */
import Component from '@ember/component';
import { set, get, computed } from '@ember/object';

export default Component.extend({
  classNames: ['te-horizontal-cards__container'],
  statsTransformed: [],
  areTwoSetsOfAnomalies: null, // passed in by parent

  oneCardOnly: computed(
    'statsTransformed',
    function() {
      return (this.get('statsTransformed').length < 2);
    }
  ),

  /**
   * Transform the stats array passed to the component
   */
  didReceiveAttrs() {
    set(this,
      'statsTransformed',
      this.statsBuilder(get(this, 'stats')));
  },

  /**
   * Given an array of values, configure the cards
   * @method statsBuilder
   * @param {Array.Array<String>} - entries of each stats card (i.e. [['entry1', ...], ['entry2', ...], ['entry3], ...])
   * @return {Object[]} - array of objects, each of which represents a stats card
   * @example
   * [[
   *    'title',
   *    'description',
   *    7,
   *    'digit',
   *    7,
   *    3
   *  ], [
   *    'title',
   *    'description',
   *    87.1,
   *    'percent',
   *    12.3,
   *    87.1
   *  ], [
   *    'title',
   *    'description',
   *    87.1,
   *    'percent',
   *    87.1,
   *    13.2
   * ]];
   */
  statsBuilder(statsArray) {
    const props = ['title', 'description', 'value', 'unit', 'old', 'new'];
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
