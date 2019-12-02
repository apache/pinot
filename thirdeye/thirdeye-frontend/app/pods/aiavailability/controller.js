import _ from 'lodash';
import moment from 'moment';
import { computed, set } from '@ember/object';
import Controller from '@ember/controller';
import { inject as service } from '@ember/service';
import { toIso } from 'thirdeye-frontend/utils/utils';

const DATE_FORMAT = 'MMM DD, YYYY';

export default Controller.extend({

  /**
   * Make duration service accessible
   */
  durationCache: service('services/duration'),

  formattedTime: computed(
    'startDate',
    'endDate',
    function() {
      const {
        startDate,
        endDate
      } = this.getProperties('startDate', 'endDate');
      const times = {};
      times.start = `${moment(toIso(startDate)).format(DATE_FORMAT)}`;
      times.end = `${moment(toIso(endDate)).format(DATE_FORMAT)}`;
      return times;
    }
  ),

  /**
   * List of anomalies to render as rows
   * @type {Array}
   */
  rows: computed(
    'tableData',
    'tableHeaders',
    'selectedSortMode',
    function() {
      const {
        tableData,
        selectedSortMode
      } = this.getProperties('tableData', 'selectedSortMode');
      let allRows = tableData;
      if (selectedSortMode) {
        let [ sortHeader, sortDir ] = selectedSortMode.split(':');

        if (sortDir === 'up') {
          allRows = _.sortBy(allRows, sortHeader);
        } else {
          allRows = _.sortBy(allRows, sortHeader).reverse();
        }
      }

      return allRows;
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
      const tableHeaders = this.get('tableHeaders');
      const propName = 'sortColumn' + sortKey.capitalize() + 'Up' || '';
      let direction = '';
      this.toggleProperty(propName);
      direction  = this.get(propName) ? 'up' : 'down';
      for (let i = 0; i < tableHeaders.length; i++) {
        if (tableHeaders[i].text === sortKey) {
          set(tableHeaders[i], 'sort', direction);
          break;
        }
      }
      this.set('selectedSortMode', `${sortKey}:${direction}`);
    }
  }
});
