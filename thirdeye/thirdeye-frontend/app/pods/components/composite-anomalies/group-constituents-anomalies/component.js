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
    component: 'custom/composite-anomalies-table/group-name',
    propertyName: 'groupName',
    title: 'Group Name'
  },
  {
    component: 'custom/composite-anomalies-table/criticality',
    propertyName: 'criticalityScore',
    title: 'Criticality Score'
  },
  {
    component: 'custom/composite-anomalies-table/current-predicted',
    propertyName: 'currentPredicted',
    title: 'Current / Predicted'
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
  title: 'Entity', // Default Header if no title is passed in.
  noRecords: 'No Groups found',
  tableData: computed('data', function () {
    const computedTableData = [];

    if (this.data && this.data.length > 0) {
      this.data.map((d) => {
        const row = {
          anomalyId: d.id,
          startDuration: getAnomaliesStartDuration(d.startTime, d.endTime, false),
          groupName: d.groupName,
          criticalityScore: d.criticality,
          currentPredicted: d.currentPredicted,
          feedback: getFeedback(d.feedback),
          isLeaf: false
        };
        computedTableData.push(row);
      });
    }

    return computedTableData;
  }),
  tableColumns: TABLE_COLUMNS
});
