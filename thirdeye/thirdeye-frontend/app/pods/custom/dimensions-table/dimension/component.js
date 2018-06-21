/**
 * Custom model table component
 * Constructs the dynamic dimension columns for the Advanced Dimension Analysis table
 * This component will be rendered once for each dynamic dimension column
 * @module custom/dimensions-table/dimension
 *
 * @example for usage in models table columns definitions
 * {
 *   disableSorting: true,
 *   isFirstColumn: index === 0,
 *   propertyName: dimension.camelize(),
 *   title: dimension.capitalize(),
 *   isGrouped: !isLastDimension,
 *   component: 'custom/dimensions-table/dimension',
 *   className: 'rootcause-dimensions-table__column--custom'
 * }
 */
import Component from "@ember/component";
import { computed, get, set, setProperties, getWithDefault } from '@ember/object';
import { isPresent } from '@ember/utils';
import { inject as service } from '@ember/service';
import { once } from '@ember/runloop'

const CUSTOM_CLASS = 'rootcause-dimensions-table__dimension-cell';

export default Component.extend({

  /**
   * The column's cell content - dimension value
   * @type {String}
   */
  dimensionValue: null,

  /**
   * The dimension group's list of excluded dimensions, typically present in a dimension of value '(ALL)-'
   * @type {String}
   */
  excludedDimensions: null,

  /**
   * Visibility of content, based on whether its a dupe of the cell above it
   * @type {Boolean}
   */
  isValueShown: true,

  /**
   * Determines whether the current cell will have the "blank" class applied
   * @type {Boolean}
   */
  isCellHidden: false,

  /**
   * Component classes
   * @type {Array}
   */
  classNames: [CUSTOM_CLASS, `${CUSTOM_CLASS}--dynamic`],


  init() {
    this._super(...arguments);

    once(() => {
      this._formatDimensionLabel(this.record.dimensionArr, this.column);
    });
  },

  /**
   * Sets value and visibility for each individual dimension cell. Allows us to group 'groupable' columns
   * to give the table the appearance of a drill-down, or tree (basically hiding the current label
   * if it is a duplicate of the previous one)
   * @method  _formatDimensionLabel
   * @param {Array} dimensionArr - array of dimension names from root of response object
   * @param {Object} column - object containing current column properties
   * @example of dimensionArr containing dimension properties from 'this.record'
   *   [{
   *     label: 'countryCode',
   *     value: '(ALL)',
   *     isHidden: true,
   *     otherValues: null
   *   },
   *   {
   *     label: 'interfaceLocale',
   *     value: '(ALL)-',
   *     isHidden: false
   *     otherValues: 'dim1, dim2.subdim'
   *   }]
   * @returns {undefined}
   * @private
   */
  _formatDimensionLabel(dimensionArr, column) {
    // This is the cell data for this column component to render: find the index
    const filteredDimensionIndex = dimensionArr.findIndex(dimObj => dimObj.label === column.propertyName);
    // This is the cell data for this column component to render: find the object
    const filteredDimensionObj = dimensionArr[filteredDimensionIndex];
    // Look at the previous object.
    const previousDimension = dimensionArr[filteredDimensionIndex - 1];
    // Was it hidden? Does it meet the requirements to be hidden? The second through last -1 depends on visibility of its neighbor to the left.
    const canThisCellBeHidden = filteredDimensionIndex > 0 ? isPresent(previousDimension) && previousDimension.isHidden : true;
    // Roll up to the next requirements to be hidden
    const isCellHidden = column.isGrouped && filteredDimensionObj.isHidden && canThisCellBeHidden;
    // Locate the current cell's element
    const cellEl = document.querySelector(`#${this.elementId}`);

    // Add hide class to table cell (no jquery)
    if (isCellHidden && cellEl) {
      this.get('element').parentNode.classList.add(`${CUSTOM_CLASS}--blank`);
    }

    // Set required cell props
    setProperties(this, {
      isValueShown: !isCellHidden,
      dimensionValue: filteredDimensionObj.value,
      excludedDimensions: filteredDimensionObj.otherValues
    });
  }

});
