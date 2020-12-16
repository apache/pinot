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
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';

const TABLE_COLUMNS = [
  {
    template: 'custom/composite-animalies-table/start-duration',
    propertyName: 'startDuration',
    title: `Start / Duration (${moment.tz([2012, 5], config.timeZone).format('z')})`
  },
  {
    template: 'custom/composite-animalies-table/anomalies-list',
    propertyName: 'details',
    title: 'Anomalies details'
  },
  {
    component: 'custom/composite-animalies-table/resolution',
    propertyName: 'feedback',
    title: 'Feedback '
  }
];

export default Component.extend({
  data: EmberArray(),
  tagName: 'section',
  customClasses: {
    table: 'composite-anomalies-table'
  },
  title: 'Composite Anomalies', // Default Header if no title is passed in.
  noRecords: 'No Composite Anomalies found',
  tableData: computed('data', function () {
    let computedTableData = [];

    if (this.data && this.data.length > 0) {
      this.data.map((d) => {
        const row = {
          startDuration: this.getAnomaliesStartDuration(d.startTime, d.endTime),
          anomaliesDetails: this.getAnomaliesDetails(d.details),
          feedback: this.getFeedback(d.feedback)
        };
        computedTableData.push(row);
      });
    }
    return computedTableData;
  }),
  tableColumns: TABLE_COLUMNS,
  /*
   *  convert anomaly 'start' and 'end' into an object to be used by template: 'custom/composite-animalies-table/start-duration'
   *
   * @param {Number} start
   * The start time in milliseconds.
   * @param {Number} end
   * The end time in milliseconds
   *
   * @returns {Object}
   * Description of the object.
   */
  getAnomaliesStartDuration: (start, end) => {
    return {
      startTime: start,
      endTime: end,
      duration: moment.duration(end - start).asHours() + ' hours'
    };
  },
  /*
   *  convert list of anonalies object in to an array of objects to be used by template: 'custom/composite-animalies-table/anomalies-list'
   *
   * @param {Object} details
   * The object containing the list of anomalies and their count.
   *
   * @returns {Array}
   * Description of the object.
   */
  getAnomaliesDetails: (anomalies) => {
    return Object.entries(anomalies).reduce((anomalyList, anomalyDetails) => {
      anomalyList.push({ name: anomalyDetails[0], count: anomalyDetails[1] });
      return anomalyList;
    }, []);
  },
  /*
   * return feedbackObject with pre-selected 'feedback' to be use in the feedback dropdown
   *
   * @param {String} feedback
   * The object containing the list of anomalies and their count.
   *
   * @returns {Object} feedbackObject
   * Description of the object.
   */
  getFeedback: (feedback) => {
    const selectedFeedback = feedback
      ? anomalyUtil.anomalyResponseObj.find((f) => f.value === feedback)
      : anomalyUtil.anomalyResponseObj[0];
    const feedbackObject = {
      options: anomalyUtil.anomalyResponseObj.mapBy('name'),
      selected: selectedFeedback ? selectedFeedback.name : anomalyUtil.anomalyResponseObj[0].name
    };
    return feedbackObject;
  }
});
