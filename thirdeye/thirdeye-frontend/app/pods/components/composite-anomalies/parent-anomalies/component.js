/*
 * Parent Anomalies Component
 *
 * Display a table which contains composite anomalies
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

import moment from 'moment';
import config from 'thirdeye-frontend/config/environment';

let data = [];

/*
 *  convert animaly 'start' and 'end' into an object to be used by template: 'custom/composite-animalies-table/start-duration'
 */
const getAnomaliesStartDuration = (start, end) => {
  return {
    startTime: start,
    endTime: end,
    duration: moment.duration(end - start).asHours() + ' hours'
  };
};

/*
 *  convert list of anonalies object in to a objects
 */
const getAnomaliesDetails = (details) => {
  const detailsObject = [];
  Object.entries(details).forEach((detail) => {
    detailsObject.push({ name: detail[0], count: detail[1] });
  });

  return detailsObject;
};

/*
    map feedback onto a feedbackObject to be use in the feedback dropdown
    This code assumes that `feedback` is either null when it has not be set, or an integer that can then be mapped to feedbackOptions.
*/
const getFeedback = (feedback = 0) => {
  const feedbackOptions = [
    'Not reviewed yet',
    'Yes - unexpected',
    'Expected temporary change',
    'Expected permanent change',
    'No change observed'
  ];
  const selectedFeedback = feedback ? feedbackOptions[feedback] : feedbackOptions[0];

  const feedbackObject = {
    options: feedbackOptions,
    selected: selectedFeedback
  };

  return feedbackObject;
};

export default Component.extend({
  didReceiveAttrs() {
    data = this.data;
  },
  tagName: 'section',
  customClasses: {
    table: 'parent-anomalies-table'
  },
  title: 'Composite Anomalies', // Default Header if no title is passed in.
  noRecords: 'No Composite Anomalies found',
  tableData: computed('data', () => {
    let computedTableData = [];

    if (data && data.length > 0) {
      data.map((d) => {
        const row = {
          startDuration: getAnomaliesStartDuration(d.startTime, d.endTime),
          anomalies: getAnomaliesDetails(d.details),
          feedback: getFeedback(d.feedback)
        };
        computedTableData.push(row);
      });
    }

    return computedTableData;
  }),
  tableColumns: computed(function () {
    const startDurationColumn = [
      {
        template: 'custom/composite-animalies-table/start-duration',
        propertyName: 'startDuration',
        title: `Start / Duration (${moment.tz([2012, 5], config.timeZone).format('z')})`
      }
    ];

    const anomiliesDetailsColumn = [
      {
        template: 'custom/composite-animalies-table/anomalies-list',
        propertyName: 'details',
        title: 'Anomalies details'
      }
    ];

    const feedbackColumn = [
      {
        component: 'custom/composite-animalies-table/resolution',
        propertyName: 'feedback',
        title: 'Feedback '
      }
    ];
    return [...startDurationColumn, ...anomiliesDetailsColumn, ...feedbackColumn];
  })
});
