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

import {
  getAnomaliesStartDuration,
  getFeedback,
  ENTITY_METRICS_COLUMNS
} from 'thirdeye-frontend/utils/composite-anomalies';

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
      this.data.map(({ id, startTime, endTime, metric, dimensions, currentPredicted, feedback }) => {
        const row = {
          id: id,
          startDuration: getAnomaliesStartDuration(startTime, endTime, false),
          metric: metric,
          dimensions: dimensions,
          currentPredicted: currentPredicted,
          feedback: getFeedback(feedback),
          isLeaf: true
        };
        computedTableData.push(row);
      });
    }

    return computedTableData;
  }),
  tableColumns: ENTITY_METRICS_COLUMNS
});
