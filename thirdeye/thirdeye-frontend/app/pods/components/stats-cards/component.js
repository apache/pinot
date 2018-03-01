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

export default Component.extend({
  classNames: ['te-horizontal-cards__container']
});
