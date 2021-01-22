/*
 * Parent Anomalies Component
 *
 * Display a table containing composite anomalies
 * @module composite-anomalies/parent-anomalies
 * @property {string} title   - Heading to use on the table
 * @property {object[]} data  - [required] array of composite anomalies objects
 *
 * @example
 * {{composite-anomalies/parent-anomalies title=<title> data=<data>}}
 *
 * @exports composite-anomalies/parent-anomalies
 */

import Component from '@ember/component';
import { computed } from '@ember/object';
import { A as EmberArray } from '@ember/array';

import moment from 'moment';
import config from 'thirdeye-frontend/config/environment';
import { getAnomaliesStartDuration, getFeedback } from 'thirdeye-frontend/utils/composite-anomalies';

const TABLE_COLUMNS = [
  {
    component: 'custom/composite-anomalies-table/start-duration',
    propertyName: 'startDuration',
    title: `Start / Duration (${moment.tz([2012, 5], config.timeZone).format('z')})`
  },
  {
    template: 'custom/composite-anomalies-table/anomalies-list',
    propertyName: 'anomaliesDetails',
    title: 'Anomalies details'
  },
  {
    component: 'custom/composite-anomalies-table/resolution',
    propertyName: 'feedback',
    title: 'Feedback'
  }
];

const CUSTOM_TABLE_CLASSES = {
  table: 'composite-anomalies-table'
};

export default Component.extend({
  data: EmberArray(),
  tagName: 'section',
  customClasses: CUSTOM_TABLE_CLASSES,
  title: 'Composite Anomalies', // Default Header if no title is passed in.
  noRecords: 'No Composite Anomalies found',
  tableData: computed('data', function () {
    const computedTableData = [];

    if (this.data && this.data.length > 0) {
      this.data.map((d) => {
        const row = {
          anomalyId: d.id,
          startDuration: getAnomaliesStartDuration(d.startTime, d.endTime, true),
          anomaliesDetails: this.getAnomaliesDetails(d.details),
          feedback: getFeedback(d.feedback),
          isLeaf: false
        };
        computedTableData.push(row);
      });
    }
    return computedTableData;
  }),
  tableColumns: TABLE_COLUMNS,
  /*
   * Convert list of anonalies object in to an array of objects to be used by template: 'custom/composite-animalies-table/anomalies-list'
   *
   * @param {Object} details
   *   The object containing the list of anomalies and their count.
   *
   * @returns {Array}
   *   Description of the object.
   */
  getAnomaliesDetails: (anomalies) => {
    return Object.entries(anomalies).reduce((anomalyList, anomalyDetails) => {
      anomalyList.push({ name: anomalyDetails[0], count: anomalyDetails[1] });
      return anomalyList;
    }, []);
  }
});
