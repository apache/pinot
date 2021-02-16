/**
 * Cube-Tooltip Component
 * Displays the tooltip contents for Cube Algorithm formulas and wiki
 * (should be placed inside a tooltip or bs-popover)
 * @example
 * {{#bs-popover}}
 *  {{cube-tooltip}}
 * {{/bs-popover}}
 * @exports cube-tooltip
 */
import Component from '@ember/component';
import config from 'thirdeye-frontend/config/environment';

export default Component.extend({
  actions: {
    /**
     * Links to cube algorithm wiki
     */
    triggerDoc() {
      window.open(config.docs.cubeWiki);
    }
  }
});
