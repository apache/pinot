import modelsTable from 'ember-models-table/components/models-table';
import _ from 'lodash'

/**
 * We are overriding ember-models-table to allow the table to be responsive to external changes
 * In the case of the rca page, when the legend (external component) changes the items selected in the table,
 * the table's selected items should reflect this
 */
export default modelsTable.extend({

  classNames: ['events-table'],

  // Necessary to avoid displaying a warning in console (false positive)
  multipleSelected: true,

  /**
   * Internal cache for preselected items
   * @type {object[]}
   */
  _preselectedItemsCache: null,

  /**
   * Internal cache for columns
   * @type {object[]}
   */
  _columnsCache: null,

  /**
   * Overriding ember-models-table API
   * Allows preselectedItems and columns to listen to changes, rather than only being assigned on init
   */
  didReceiveAttrs() {
    const { preselectedItems, _preselectedItemsCache, columns, _columnsCache } =
      this.getProperties('preselectedItems', '_preselectedItemsCache', 'columns', '_columnsCache');

    if (!_.isEqual(_preselectedItemsCache, preselectedItems)) {
      this._setupSelectedRows();
      this.set('_preselectedItemsCache', _.cloneDeep(preselectedItems));
    }

    if (!_.isEqual(_columnsCache, columns)) {
      this._setupColumns();
      this.set('_columnsCache', _.cloneDeep(columns));
    }
  }

});
