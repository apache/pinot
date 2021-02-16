/*
 * Group Constituents Anomalies Component
 *
 * Display a table containing group constituents anomalies
 * @module composite-anomalies/group-constituents-anomalies
 * @property {string} title - Heading to use on the table
 * @property {object[]} data - [required] array of composite anomalies objects
 *
 * @example
 * {{composite-anomalies/group-constituents-anomalies title=<title> data=<data>}}
 *
 * @exports composite-anomalies/group-constituents-anomalies
 */

import Component from '@ember/component';
import { computed } from '@ember/object';
import { A as EmberArray } from '@ember/array';

import {
  getAnomaliesStartDuration,
  getFeedback,
  GROUP_CONSTITUENTS_COLUMNS
} from 'thirdeye-frontend/utils/composite-anomalies';

const CUSTOM_TABLE_CLASSES = {
  table: 'composite-anomalies-table'
};

export default Component.extend({
  data: EmberArray(),
  tagName: 'section',
  customClasses: CUSTOM_TABLE_CLASSES,
  title: 'Entity', // Default Header if no title is passed in.
  noRecords: 'No Groups found',
  tableData: computed('data', function () {
    const computedTableData = [];

    if (this.data && this.data.length > 0) {
      this.data.map(({ id, startTime, endTime, groupName, criticality, currentPredicted, feedback }) => {
        const row = {
          anomalyId: id,
          startDuration: getAnomaliesStartDuration(startTime, endTime, false),
          groupName: groupName,
          criticalityScore: criticality,
          currentPredicted: currentPredicted,
          feedback: getFeedback(feedback),
          isLeaf: false
        };
        computedTableData.push(row);
      });
    }

    return computedTableData;
  }),
  tableColumns: GROUP_CONSTITUENTS_COLUMNS
});
