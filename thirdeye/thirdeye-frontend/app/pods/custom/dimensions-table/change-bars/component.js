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

export default Component.extend({

  dimensionLabel: null,
  isCellHidden: false,
  isFirstColumn: false,
  classNames: ['rootcause-dimensions-table__dimension-cell'],
  isRowSelected: false,
  onSelection: null,
  inputId: null,
  isNegativeChange: false,

  init() {
    this._super(...arguments);
    this._setBarWidths();
  },

  _setBarWidths() {
    const negativeMax = d3.max(this.data.map(row => toWidthNumber(row.elementWidth.negative)));
    const positiveMax = d3.max(this.data.map(row => toWidthNumber(row.elementWidth.positive)));

    // Map the max values to a slightly larger scale
    const containerWidthScale = d3.scale.linear()
      .domain([0, 100])
      .range([0, 120]);

    // Take the max value and map it to a width of 150 pixels. This will be the container width for each column.
    const containerWidthNegative = Math.round(containerWidthScale(negativeMax));
    const containerWidthPositive = Math.round(containerWidthScale(positiveMax));

    // Map the negative bar width to the largest negative value as 100%
    const negativeBarScale = d3.scale.linear()
      .domain([0, negativeMax])
      .range([0, 100]);

    // Map the positive bar width to the largest positive value as 100%
    const positiveBarScale = d3.scale.linear()
      .domain([0, positiveMax])
      .range([0, 100]);

    setProperties(this, {
      containerWidthNegative,
      containerWidthPositive,
      isNegativeChange: toWidthNumber(this.record.contributionToOverallChange) < 0,
      barWidthNegative: negativeBarScale(toWidthNumber(this.record.elementWidth.negative)),
      barWidthPositive: positiveBarScale(toWidthNumber(this.record.elementWidth.positive))
    });
  }

});
