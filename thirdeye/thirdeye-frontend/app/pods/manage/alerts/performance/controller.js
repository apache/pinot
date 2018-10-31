import _ from 'lodash';
import moment from 'moment';
import { computed, set } from '@ember/object';
import Controller from '@ember/controller';
import { inject as service } from '@ember/service';

export default Controller.extend({

  /**
   * Make duration service accessible
   */
  durationCache: service('services/duration'),

  /**
   * Active class appendage of 'view totals' link
   * @type {String}
   */
  viewTotalsState: computed('viewTotals', function() {
    return this.get('viewTotals') ? 'active' : 'inactive';
  }),

  /**
   * Active class appendage of 'view average' link
   * @type {String}
   */
  viewAvgState: computed('viewTotals', function() {
    return !this.get('viewTotals') ? 'active' : 'inactive';
  }),

  /**
   * List of applications to render as rows, with filtering applied
   * @type {Array}
   */
  applications: computed(
    'perfDataByApplication',
    'selectedSortMode',
    'viewTotals',
    'sortMap',
    function() {
      const sortMap = this.get('sortMap');
      const selectedSortMode = this.get('selectedSortMode');
      const sortMapChild = this.get('viewTotals') ? '.tot' : '.avg';
      const keysWithChildren = ['anomaly', 'user', 'responses'];
      let allApps = this.get('perfDataByApplication');
      let fullSortKey = '';
      let applySortType = true;

      if (selectedSortMode) {
        let [ sortKey, sortDir ] = selectedSortMode.split(':');
        applySortType = keysWithChildren.includes(sortKey);
        fullSortKey = applySortType ? sortMap[sortKey] + sortMapChild : sortMap[sortKey];

        if (sortDir === 'up') {
          allApps = _.sortBy(allApps, fullSortKey);
        } else {
          allApps = _.sortBy(allApps, fullSortKey).reverse();
        }
      }

      return allApps;
    }
  ),

  actions: {

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(rangeOption) {
      const {
        start,
        end,
        value: duration
      } = rangeOption;
      const durationObj = {
        duration,
        startDate: moment(start).valueOf(),
        endDate: moment(end).valueOf()
      };
      // Cache the new time range and update page with it
      this.get('durationCache').setDuration(durationObj);
      this.transitionToRoute({ queryParams: durationObj });
    },

    /**
     * Handle sorting for each sortable table column
     * @param {String} sortKey  - stringified start date
     */
    toggleSortDirection(sortKey) {
      const sortMenu = this.get('sortMenuGlyph');
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';
      let direction = '';
      this.toggleProperty(propName);
      direction  = this.get(propName) ? 'up' : 'down';
      set(sortMenu, sortKey, direction);
      this.set('selectedSortMode', `${sortKey}:${direction}`);
    }
  }
});
