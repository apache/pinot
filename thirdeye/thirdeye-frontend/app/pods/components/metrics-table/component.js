import Component from '@ember/component';
import { computed } from '@ember/object';
import { makeSortable, toMetricLabel, toColorDirection, isInverse } from 'thirdeye-frontend/utils/rca-utils';
import { humanizeScore } from 'thirdeye-frontend/utils/utils';

export default Component.extend({
  classNames: ['metrics-table'],

  /**
   * Columns for metrics table
   * @type Object[]
   */
  metricsTableColumns: [
    {
      template: 'custom/table-checkbox'
    }, {
      propertyName: 'label',
      title: 'Metric',
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--large'
    }, {
      template: 'custom/rca-metric-links',
      propertyName: 'links',
      title: 'Links',
      disableFiltering: true,
      className: 'rootcause-metric__table__links-column rootcause-metric__table__links-column--small'
    }, {
      propertyName: 'score',
      title: 'Anomalous',
      disableFiltering: true,
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--small',
      sortPrecedence: 0,
      sortDirection: 'desc'
    }, {
      propertyName: 'wo1w',
      template: 'custom/metrics-table-changes/wo1w',
      sortedBy: 'sortable_wo1w',
      title: 'WoW',
      disableFiltering: true,
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--small'
    }, {
      propertyName: 'wo2w',
      template: 'custom/metrics-table-changes/wo2w',
      sortedBy: 'sortable_wo2w',
      title: 'Wo2W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--small'
    }, {
      propertyName: 'wo3w',
      template: 'custom/metrics-table-changes/wo3w',
      sortedBy: 'sortable_wo3w',
      title: 'Wo3W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--small'
    }, {
      propertyName: 'wo4w',
      template: 'custom/metrics-table-changes/wo4w',
      sortedBy: 'sortable_wo4w',
      title: 'Wo4W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column rootcause-metric__table__links-column--small'
    }
  ],

  /**
   * Metric urns in sorted order
   * @type {string}
   */
  urns: null,

  /**
   * Entities cache
   * @type {object}
   */
  entities: null,

  /**
   * Relative changes from offset to current
   * @type {object}
   */
  changesOffset: null,

  /**
   * Formatted strings for changesOffset
   * @type {object}
   */
  changesOffsetFormatted: null,

  /**
   * User-selected urns
   * @type {Set}
   */
  selectedUrns: null,

  /**
   * (External) links for metric labels
   * @type {object}
   */
  links: null,

  /**
   * Scores for metric entities
   * @type {object}
   */
  scores: null,

  /**
   * Data for metrics table
   * @type Object[] - array of objects, each corresponding to a row in the table
   */
  metricsTableData: computed(
    'urns',
    'selectedUrns',
    'entities',
    'changesOffset',
    'changesOffsetFormatted',
    'links',
    'scores',
    function() {
      const { urns, entities, changesOffset, selectedUrns, links, scores } =
        this.getProperties('urns', 'entities', 'changesOffset', 'selectedUrns', 'links', 'scores');

      return urns.map(urn => {
        return {
          urn,
          links: links[urn],
          isSelected: selectedUrns.has(urn),
          label: toMetricLabel(urn, entities),
          score: humanizeScore(scores[urn]),
          wo1w: this._makeRecord('wo1w', urn),
          wo2w: this._makeRecord('wo2w', urn),
          wo3w: this._makeRecord('wo3w', urn),
          wo4w: this._makeRecord('wo4w', urn),
          sortable_wo1w: makeSortable(changesOffset['wo1w'][urn]),
          sortable_wo2w: makeSortable(changesOffset['wo2w'][urn]),
          sortable_wo3w: makeSortable(changesOffset['wo3w'][urn]),
          sortable_wo4w: makeSortable(changesOffset['wo4w'][urn])
        };
      });
    }
  ),

  _makeRecord(offset, urn) {
    const { entities, changesOffset, changesOffsetFormatted } =
      this.getProperties('entities', 'changesOffset', 'changesOffsetFormatted');
    return {
      value: changesOffsetFormatted[offset][urn],
      direction: toColorDirection(changesOffset[offset][urn], isInverse(urn, entities))
    };
  },

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
