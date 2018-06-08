/**
 * Custom model table component
 * Constructs the relative dimension contribution bar width for dimensions table
 * @module custom/dimensions-table/change-bars
 *
 * @example for usage in models table columns definitions
 *  {
 *    propertyName: 'contributionToOverallChange',
 *    component: 'custom/dimensions-table/change-bars',
 *    title: 'Contribution to Overall Change',
 *    className: 'rootcause-dimensions-table__column rootcause-dimensions-table__column--bar-cell',
 *    disableSorting: true,
 *    disableFiltering: true
 *  }
 */
import Component from "@ember/component";
import { getWithDefault } from '@ember/object';
import { get, set, setProperties } from '@ember/object';
import { inject as service } from '@ember/service';
import { toWidthNumber } from 'thirdeye-frontend/utils/rca-utils';
import { once } from '@ember/runloop';
import d3 from 'd3';

const BLOCK_CLASS = 'rootcause-dimensions-table__dimension-cell';
const BAR_CLASS = 'rootcause-dimensions-table__bar-container';

export default Component.extend({

  dimensionLabel: null,
  isCellHidden: false,
  isFirstColumn: false,
  classNames: [BLOCK_CLASS],
  isRowSelected: false,
  onSelection: null,
  inputId: null,

  didReceiveAttrs() {
    this._super(...arguments);
    this._setBarWidths();
  },

  _setBarWidths() {
    const barClassExpand = `${BAR_CLASS}--expand`;
    const barClassCollapse = `${BAR_CLASS}--collapse`;
    const negativeMax = d3.max(this.data.map(row => toWidthNumber(row.elementWidth.negative)));
    const positiveMax = d3.max(this.data.map(row => toWidthNumber(row.elementWidth.positive)));

    const containerWidthScale = d3.scale.linear()
      .domain([0, 100])
      .range(['0%', `40%`]);

    setProperties(this, {
      containerWidthNegative: containerWidthScale(negativeMax),
      barWidthNegative: toWidthNumber(this.record.elementWidth.negative) > 0 ? '100%' : '0%',
      collapseClassNegative: negativeMax === 0 ? barClassCollapse : '',
      collapseClassPositive: positiveMax === 0 ? barClassCollapse : '',
      expandClassNegative: positiveMax === 0 ? barClassExpand : '',
      expandClassPositive: negativeMax === 0 ? barClassExpand : ''
    });
  }

});
