/**
 * Custom model table component
 * Renders data for the percent-change column with some color-coding logic
 * @module custom/dimensions-table/percent-change
 */
import { set } from '@ember/object';
import Component from "@ember/component";
import { toWidthNumber } from 'thirdeye-frontend/utils/rca-utils';

const CUSTOM_CLASS = 'rootcause-dimensions-table__change-cell';

export default Component.extend({

  isNegativeChange: false,
  classNames: [CUSTOM_CLASS],
  classNameBindings: [`isNegativeChange:${CUSTOM_CLASS}--negative`],

  init() {
    this._super(...arguments);
    if (toWidthNumber(this.record.percentageChange) < 0) {
      set(this, 'isNegativeChange', true);
    }
  }
});
