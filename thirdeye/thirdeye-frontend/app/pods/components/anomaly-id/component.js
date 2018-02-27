/**
 * Handles display of anomaly ID and associated metadata
 * @module components/anomaly-id
 * @exports anomaly-id
 */
import { computed } from '@ember/object';

import Component from '@ember/component';

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
  anomalyChangeRate: computed('anomaly.current', 'anomaly.baseline', function() {
    const currentValue = this.get('anomaly.current') || 0;
    const baselineValue = this.get('anomaly.baseline') || 0;

    if (baselineValue !== 0) {
      return ((currentValue - baselineValue) / baselineValue * 100).toFixed(2);
    } else {
      return 0;
    }
  })
});
