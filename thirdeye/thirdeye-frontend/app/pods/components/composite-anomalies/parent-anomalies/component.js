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

import {
  getAnomaliesStartDuration,
  getFeedback,
  PARENT_TABLE_COLUMNS
} from 'thirdeye-frontend/utils/composite-anomalies';

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
      this.data.map(({ id, startTime, endTime, details, feedback }) => {
        const row = {
          anomalyId: id,
          startDuration: getAnomaliesStartDuration(startTime, endTime, true),
          anomaliesDetails: details,
          feedback: getFeedback(feedback),
          isLeaf: false
        };
        computedTableData.push(row);
      });
    }
    return computedTableData;
  }),
  tableColumns: PARENT_TABLE_COLUMNS
});
