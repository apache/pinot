/**
 * Anomaly-Stats-Block Component
 * Displays a row of cards, each of which contains stats, depending on the anomalyStats that are passed to the component
 * @module components/anomaly-stats-block
 * @property {boolean} isTunePreviewActive  - [optional] flag for whether tuning preview is active
 * @property {string} displayMode           - [optional] mode of display (i.e. "explore")
 * @property {object[]} anomalyStats        - [required] array of stats object that specify what to display on each card
 *
 * @example
 * {{anomaly-stats-block
 *   isTunePreviewActive=isTunePreviewActive
 *   displayMode="explore"
 *   anomalyStats=anomalyStats}}
 *
 * @exports anomaly-stats-block
 */
import Component from '@ember/component';

export default Component.extend({
  classNames: ['te-horizontal-cards__container']
});
