import { computed } from '@ember/object';
import Component from '@ember/component';
import _ from 'lodash';
import moment from 'moment';
import { toEventLabel } from 'thirdeye-frontend/utils/rca-utils';

export default Component.extend({
  columns: null, // []

  entities: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (e)

  /**
   * Returns a list of record objects to display in the table
   * Sets the 'isSelected' property to respond to table selection
   * @type {Object} - object with keys as urns and values as entities
   */
  records: computed(
    'entities',
    'selectedUrns',
    function () {
      const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');

      const records = _.cloneDeep(entities);
      Object.keys(records).forEach(urn => records[urn].isSelected = selectedUrns.has(urn));
      return records;
    }
  ),

  /**
   * Returns a list of values to display in the rootcause table
   * Add human readable date properties to the list of values
   * @type {Array[Objects]} - array of entities
   */
  data: computed(
    'records',
    function () {
      let values = Object.values(this.get('records'));
      let format = 'ddd, MMM DD hh:mm A';

      values.forEach((value) => {
        value.humanStart = moment(value.start).format(format);
        // If this is an ongoing event
        if (value.end <= 0) {
          value.duration = 'ongoing';
        } else {
          value.label = toEventLabel(value.urn, this.get('records'));
          let timeDiff = value.end - value.start;
          let duration = moment.duration(timeDiff).days();
          value.duration = duration > 1 ? `${duration} days` : `${duration} day`;
        }
      });

      return values;
    }
  ),

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed(
    'records',
    'selectedUrns',
    function () {
      const { records, selectedUrns } = this.getProperties('records', 'selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => records[urn]).map(urn => records[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    /**
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      const { records, selectedUrns, onSelection } = this.getProperties('records', 'selectedUrns', 'onSelection');
      if (onSelection) {
        const table = new Set(e.selectedItems.map(e => e.urn));
        const added = [...table].filter(urn => !selectedUrns.has(urn));
        const removed = [...selectedUrns].filter(urn => records[urn] && !table.has(urn));

        const updates = {};
        added.forEach(urn => updates[urn] = true);
        removed.forEach(urn => updates[urn] = false);

        onSelection(updates);
      }
    }
  }
});
