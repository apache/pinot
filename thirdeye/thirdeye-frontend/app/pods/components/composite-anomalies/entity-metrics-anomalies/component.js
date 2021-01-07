/*
 * Entity Metric Anomalies Component
 *
 * Display a table containing entity metric anomalies
 * @module composite-anomalies/entity-metric-anomalies
 * @property {string} title - Heading to use on the table
 * @property {object[]} data - [required] array of composite anomalies objects
 *
 * @example
 * {{composite-anomalies/entity-metric-anomalies title=<title> data=<data>}}
 *
 * @exports composite-anomalies/entity-metric-anomalies
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
    component: 'custom/composite-anomalies-table/metric',
    propertyName: 'metric',
    title: 'Metric'
  },
  {
    component: 'custom/composite-anomalies-table/dimensions',
    propertyName: 'dimensions',
    title: 'Dimension'
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
  },
  {
    component: 'custom/anomalies-table/investigation-link',
    propertyName: 'id',
    title: 'Investigate'
  }
];

const CUSTOM_TABLE_CLASSES = {
  table: 'composite-anomalies-table'
};

export default Component.extend({
  data: EmberArray(),
  tagName: 'section',
  customClasses: CUSTOM_TABLE_CLASSES,
  title: 'Metrics Anomalies',
  noRecords: 'No Entity Metrics found',
  tableData: computed('data', function () {
    const computedTableData = [];

    if (this.data && this.data.length > 0) {
      this.data.map((d) => {
        const row = {
          id: d.id,
          startDuration: getAnomaliesStartDuration(d.startTime, d.endTime, false),
          metric: d.metric,
          dimensions: d.dimensions,
          currentPredicted: d.currentPredicted,
          feedback: getFeedback(d.feedback)
        };
        computedTableData.push(row);
      });
    }

    return computedTableData;
  }),
  tableColumns: TABLE_COLUMNS
});
