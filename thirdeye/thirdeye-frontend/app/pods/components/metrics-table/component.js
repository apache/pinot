import Component from '@ember/component';
import { computed } from '@ember/object';
import { makeSortable } from 'thirdeye-frontend/helpers/utils';

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
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'changeFormatted',
      sortedBy: 'changeSortable',
      title: 'baseline',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'change1wFormatted',
      sortedBy: 'change1wSortable',
      title: 'WoW',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'change2wFormatted',
      sortedBy: 'change2wSortable',
      title: 'Wo2W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'change3wFormatted',
      sortedBy: 'change3wSortable',
      title: 'Wo3W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'change4wFormatted',
      sortedBy: 'change4wSortable',
      title: 'Wo4W',
      disableFiltering: true,
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
    'changesOffset',
    'changesOffsetFormatted',
    function() {
      let arr = [];
      const { urns, metrics, scores, changesOffset, changesOffsetFormatted, selectedUrns } =
        this.getProperties('urns', 'metrics', 'scores', 'changesOffset', 'changesOffsetFormatted', 'selectedUrns');

      urns.forEach(urn => {
        arr.push({
          urn,
          isSelected: selectedUrns.has(urn),
          metric: metrics[urn],
          score: scores[urn],
          change: changesOffset['baseline'][urn],
          change1w: changesOffset['wo1w'][urn],
          change2w: changesOffset['wo2w'][urn],
          change3w: changesOffset['wo3w'][urn],
          change4w: changesOffset['wo4w'][urn],
          changeFormatted: changesOffsetFormatted['baseline'][urn],
          change1wFormatted: changesOffsetFormatted['wo1w'][urn],
          change2wFormatted: changesOffsetFormatted['wo2w'][urn],
          change3wFormatted: changesOffsetFormatted['wo3w'][urn],
          change4wFormatted: changesOffsetFormatted['wo4w'][urn],
          changeSortable: makeSortable(changesOffset['baseline'][urn]),
          change1wSortable: makeSortable(changesOffset['wo1w'][urn]),
          change2wSortable: makeSortable(changesOffset['wo2w'][urn]),
          change3wSortable: makeSortable(changesOffset['wo3w'][urn]),
          change4wSortable: makeSortable(changesOffset['wo4w'][urn])
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
