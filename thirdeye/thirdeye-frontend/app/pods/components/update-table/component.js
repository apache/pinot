import modelsTable from 'ember-models-table/components/models-table';

/**
 * We are overriding ember-models-table to allow the table to be responsive to external changes
 * In the case of the rca page, when the legend (external component) changes the items selected in the table,
 * the table's selected items should reflect this
 */
export default modelsTable.extend({

  classNames: ['events-table'],

  // Necessary to avoid displaying a warning in console (false positive)
  multipleSelected: true,

  // Necessary to receive displayDataChanged action
  sendDisplayDataChangedAction: true,

  /**
   * Overriding ember-models-table API
   * Allows preselectedItems and columns to listen to changes, rather than only being assigned on init
   */
  didReceiveAttrs() {
    this._setupSelectedRows();
    this._setupColumns();
  }
});
