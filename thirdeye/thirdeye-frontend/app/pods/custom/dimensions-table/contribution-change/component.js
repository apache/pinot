/**
 * Custom model table component
 * Renders data for the contribution-change column with some color-coding logic
 * @module custom/dimensions-table/contribution-change
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
    if (toWidthNumber(this.record.contributionChange) < 0) {
      set(this, 'isNegativeChange', true);
    }
  }
});

