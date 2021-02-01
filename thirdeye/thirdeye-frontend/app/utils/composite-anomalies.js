/*
 * Util functions to structure the data to be passed into individual columns
 */

import moment from 'moment';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { ANOMALIES_START_DISPLAY_FORMAT } from 'thirdeye-frontend/utils/constants';
import { checkForMatch } from 'thirdeye-frontend/utils/utils';
import config from 'thirdeye-frontend/config/environment';

const COMMON_COLUMNS = {
  START_DURATION: {
    component: 'custom/composite-anomalies-table/start-duration',
    propertyName: 'startDuration',
    title: `Start / Duration (${moment.tz([2012, 5], config.timeZone).format('z')})`,
    filterFunction(cell, filterStr, record) {
      const { startTime, duration } = record[this.propertyName];

      return checkForMatch({ startTime, duration }, filterStr);
    }
  },
  FEEDBACK: {
    component: 'custom/composite-anomalies-table/resolution',
    propertyName: 'feedback',
    title: 'Feedback'
  },
  CURRENT_PREDICTED: {
    component: 'custom/composite-anomalies-table/current-predicted',
    propertyName: 'currentPredicted',
    title: 'Current / Predicted',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  }
};

export const PARENT_TABLE_COLUMNS = [
  COMMON_COLUMNS.START_DURATION,
  {
    template: 'custom/composite-anomalies-table/anomalies-list',
    propertyName: 'anomaliesDetails',
    title: 'Anomalies details',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  },
  COMMON_COLUMNS.FEEDBACK
];

export const GROUP_CONSTITUENTS_COLUMNS = [
  COMMON_COLUMNS.START_DURATION,
  {
    component: 'custom/composite-anomalies-table/group-name',
    propertyName: 'groupName',
    title: 'Group Name',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  },
  {
    component: 'custom/composite-anomalies-table/criticality',
    propertyName: 'criticalityScore',
    title: 'Criticality Score',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  },
  COMMON_COLUMNS.CURRENT_PREDICTED,
  COMMON_COLUMNS.FEEDBACK
];

export const ENTITY_METRICS_COLUMNS = [
  COMMON_COLUMNS.START_DURATION,
  {
    component: 'custom/composite-anomalies-table/metric',
    propertyName: 'metric',
    title: 'Metric',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  },
  {
    component: 'custom/composite-anomalies-table/dimensions',
    propertyName: 'dimensions',
    title: 'Dimension',
    filterFunction(cell, filterStr, record) {
      return checkForMatch(record[this.propertyName], filterStr);
    }
  },
  COMMON_COLUMNS.CURRENT_PREDICTED,
  COMMON_COLUMNS.FEEDBACK,
  {
    component: 'custom/anomalies-table/investigation-link',
    propertyName: 'id',
    title: 'Investigate'
  }
];

/*
 * Convert anomaly 'start' and 'end' into an object
 *
 * @param {Number} start
 *   The start time in milliseconds.
 * @param {Number} end
 *   The end time in milliseconds
 * @param {Boolean} showLink
 *   Indicates whether to hyperlink the start duration
 *
 * @returns {Object}
 *   Description of the object.
 */
export const getAnomaliesStartDuration = (start, end, showLink = false) => {
  const timezone = moment().tz(moment.tz.guess()).format('z');

  return {
    startTime: `${moment(start).format(ANOMALIES_START_DISPLAY_FORMAT)} ${timezone}`,
    endTime: `${moment(end).format(ANOMALIES_START_DISPLAY_FORMAT)} ${timezone}`,
    duration: moment.duration(end - start).asHours() + ' hours',
    showLink
  };
};

/*
 * Return feedbackObject with pre-selected 'feedback' to be use in the feedback dropdown
 *
 * @param {String} feedback
 *   The object containing the list of anomalies and their count.
 *
 * @returns {Object} feedbackObject
 *   Description of the object.
 */
export const getFeedback = (feedback) => {
  const selectedFeedback = feedback
    ? anomalyUtil.anomalyResponseObj.find((f) => f.value === feedback.feedbackType)
    : anomalyUtil.anomalyResponseObj[0];

  return {
    options: anomalyUtil.anomalyResponseObj.mapBy('name'),
    selected: selectedFeedback ? selectedFeedback.name : anomalyUtil.anomalyResponseObj[0].name
  };
};
