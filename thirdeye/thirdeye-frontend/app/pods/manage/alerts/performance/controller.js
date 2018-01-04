/**
 * Controller for Alert Details Page: Tune Sensitivity Tab
 * @module manage/alert/tune
 * @exports manage/alert/tune
 */
import fetch from 'fetch';
import Ember from 'ember';
import _ from 'lodash';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/helpers/utils';
import Controller from '@ember/controller';

export default Controller.extend({

  /**
   * Mapping anomaly table column names to corresponding prop keys
   */
  sortMap: {
    name: 'name',
    alert: 'alerts',
    anomaly: 'data.totalAlerts',
    user: 'data.userReportAnomaly',
    responses: 'data.totalResponses',
    resrate: 'data.responseRate',
    precision: 'data.precision'
  },

  /**
   * List of applications to render as rows, with filtering applied
   */
  applications: Ember.computed(
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
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';

      this.toggleProperty(propName);
      if (this.get(propName)) {
        this.set('selectedSortMode', sortKey + ':up');
      } else {
        this.set('selectedSortMode', sortKey + ':down');
      }
    }
  }
});
