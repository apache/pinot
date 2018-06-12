/**
 * Custom model table component
 * Constructs the dynamic dimension columns for the Advanced Dimension Analysis table
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
import { getWithDefault } from '@ember/object';
import { get, set, setProperties } from '@ember/object';
import { inject as service } from '@ember/service';
import { once } from '@ember/runloop'

const BLOCK_CLASS = 'rootcause-dimensions-table__dimension-cell';

export default Component.extend({

  dimensionLabel: null,
  isCellHidden: false,
  isRowSelected: false,
  onSelection: null,
  inputId: null,
  classNames: [BLOCK_CLASS],
  classNameBindings: [`isCellHidden:${BLOCK_CLASS}--blank`],

  didReceiveAttrs() {
    this._super(...arguments);
    this._formatDimensionLabel();
  },

  _formatDimensionLabel() {
    const isFirstColumn = this.column.isFirstColumn;
    const dimNames = getWithDefault(this.record, 'names', []);
    const label = this.record[this.column.propertyName];
    const prevRecord = this.data[this.record.id - 1];
    const prevLabel = prevRecord ? prevRecord[this.column.propertyName] : '';
    const isThisLabelHidden = this.column.isGrouped && label === prevLabel;
    const modifiedLabel = label.toLowerCase().includes('all') ? 'Total' : label;

    once(() => {
      setProperties(this, {
        isFirstColumn,
        dimensionLabel: isThisLabelHidden ? '' : modifiedLabel,
        isCellHidden: isThisLabelHidden,
        inputId: `dimension-check${this.record.id}`
      });
    });
  }

});
