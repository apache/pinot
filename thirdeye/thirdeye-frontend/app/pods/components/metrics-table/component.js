import Component from '@ember/component';
import { computed } from '@ember/object';
import _ from 'lodash';

export default Component.extend({
  /**
   * Columns for metrics table
   * @type Object[]
   */
  metricsTableColumns: [
    {
      template: 'custom/table-checkbox'
    }, {
      propertyName: 'metric',
      title: 'Metric Name',
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'score',
      title: 'Anomalous Score',
      className: 'rootcause-metric__table__column'
    }, {
      template: 'custom/metrics-table-changes',
      propertyName: 'change',
      title: 'Changes',
      className: 'rootcause-metric__table__column'
    }
  ],

  /**
   * Data for metrics table
   * @type Object[] - array of objects, each corresponding to a row in the table
   */
  metricsTableData: computed(
    'selectedUrns',
    'urns',
    'metrics',
    'scores',
    'changesFormatted',
    function() {
      let arr = [];
      const properties = ['urns', 'metrics', 'scores', 'changesFormatted', 'selectedUrns'];
      const { urns, metrics, scores, changesFormatted, selectedUrns } = this.getProperties(...properties);
      const urnsClone = _.cloneDeep(urns);

      urnsClone.forEach(urn => {
        arr.push({
          urn,
          isSelected: selectedUrns.has(urn),
          metric: metrics[urn],
          score: scores[urn],
          change: changesFormatted[urn]
        });
      });
      return arr;
    }
  ),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed(
    'metricsTableData',
    'selectedUrns',
    function () {
      const { metricsTableData, selectedUrns } = this.getProperties('metricsTableData', 'selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => metricsTableData[urn]).map(urn => metricsTableData[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      const selectedItemsArr = [...e.selectedItems];
      const selectedItem = selectedItemsArr.length ? selectedItemsArr[0].urn : '';

      if (selectedItem) {
        this.get('toggleSelection')(selectedItem);
      }
    }
  }
});
