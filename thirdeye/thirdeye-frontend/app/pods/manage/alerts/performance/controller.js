/**
 * Controller for Alert Details Page: Tune Sensitivity Tab
 * @module manage/alert/tune
 * @exports manage/alert/tune
 */
import fetch from 'fetch';
import Ember from 'ember';
import _ from 'lodash';
import { computed } from '@ember/object';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';
import Controller from '@ember/controller';

export default Controller.extend({

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
     * Handle sorting for each sortable table column
     * @param {String} sortKey  - stringified start date
     */
    toggleSortDirection(sortKey) {
      const sortMenu = this.get('sortMenuGlyph');
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';
      let direction = '';
      this.toggleProperty(propName);
      direction  = this.get(propName) ? 'up' : 'down';
      Ember.set(sortMenu, sortKey, direction);
      this.set('selectedSortMode', `${sortKey}:${direction}`);
    }
  }
});
