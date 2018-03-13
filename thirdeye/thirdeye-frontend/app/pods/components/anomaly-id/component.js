/**
 * Handles display of anomaly ID and associated metadata
 * @module components/anomaly-id
 * @exports anomaly-id
 */
import { computed } from '@ember/object';
import Component from '@ember/component';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';

export default Component.extend({
  /**
   * Component's tag name
   */
  tagName: 'ul',

  /**
   * List of associated classes
   */
  classNames: ['anomaly-id'],

  /**
   * Calculate the difference between the start value and end value for the currently loaded anomaly.
   * Note: data frequency is 5min, hourly, or daily
   * @method anomalyChangeRate
   * @return {Number} - total % change from baseline
   */
  anomalyChangeRate: computed('anomaly.{current,baseline}', function() {
    const currentValue = this.get('anomaly.current') || 0;
    const baselineValue = this.get('anomaly.baseline') || 0;

    if (baselineValue !== 0) {
      return floatToPercent((currentValue - baselineValue) / baselineValue);
    } else {
      return 0;
    }
  })
});
