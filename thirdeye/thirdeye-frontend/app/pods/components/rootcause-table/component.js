import { computed } from '@ember/object';
import Component from '@ember/component';
import moment from 'moment';
import { makeTime, toEventLabel } from 'thirdeye-frontend/utils/rca-utils';
import _ from 'lodash';

const ROOTCAUSE_EVENT_DATE_FORMAT = 'ddd, MMM DD hh:mm a z';

export default Component.extend({
  columns: null, // []

  entities: null, // {}

  selectedUrns: null, // Set

  onSelection: null, // function (e)

  /**
   * Returns a list of values to display in the rootcause table
   * Add human readable date properties to the list of values
   * @type {objects[]} - array of entities
   */
  data: computed('entities', 'selectedUrns', function () {
    const { entities, selectedUrns } = this.getProperties('entities', 'selectedUrns');

    return Object.values(entities)
      .filter((e) => e.type === 'event')
      .map((e) => {
        const duration = e.end <= 0 ? Number.POSITIVE_INFINITY : e.end - e.start;

        return Object.assign({}, e, {
          isSelected: selectedUrns.has(e.urn),
          duration,
          humanStart: makeTime(e.start).format(ROOTCAUSE_EVENT_DATE_FORMAT),
          humanDuration: this._formatDuration(duration),
          label: toEventLabel(e.urn, entities)
        });
      });
  }),

  /**
   * Keeps track of items that are selected in the table
   * @type {object[]}
   */
  preselectedItems: computed('data', 'selectedUrns', function () {
    const { data, selectedUrns } = this.getProperties('data', 'selectedUrns');
    return [...selectedUrns].filter((urn) => data[urn]).map((urn) => data[urn]);
  }),

  /**
   * Helper for formatting durations specifically for events
   *
   * @param {int} duration (in millis)
   * @returns {string} formatted duration
   * @private
   */
  _formatDuration(duration) {
    if (!Number.isFinite(duration)) {
      return 'ongoing';
    }

    if (duration <= 0) {
      return '';
    }

    const d = moment.duration(duration);

    if (d.days() >= 1) {
      const days = d.days();
      return days > 1 ? `${days} days` : `${days} day`;
    }

    if (d.hours() >= 1) {
      const hours = d.hours();
      return hours > 1 ? `${hours} hours` : `${hours} hour`;
    }

    if (d.minutes() >= 1) {
      const minutes = d.minutes();
      return minutes > 1 ? `${minutes} minutes` : `${minutes} minute`;
    }
  },

  actions: {
    /**
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged(e) {
      if (_.isEmpty(e.selectedItems)) {
        return;
      }

      const { selectedUrns, onSelection } = this.getProperties('selectedUrns', 'onSelection');

      if (!onSelection) {
        return;
      }

      const urn = e.selectedItems[0].urn;
      onSelection({ [urn]: !selectedUrns.has(urn) });
    }
  }
});
