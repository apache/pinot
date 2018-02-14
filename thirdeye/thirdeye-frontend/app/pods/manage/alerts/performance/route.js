/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import { getWithDefault } from '@ember/object';

import { isPresent } from '@ember/utils';
import Route from '@ember/routing/route';
import fetch from 'fetch';
import RSVP, { hash, allSettled } from 'rsvp';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';

/**
 * If true, this reduces the list of alerts per app to 2 for a quick demo.
 */
const demoMode = false;

/**
 * Mapping anomaly table column names to corresponding prop keys
 */
const sortMap = {
  name: 'name',
  alert: 'alerts',
  anomaly: 'data.totalAlerts',
  user: 'data.userReportAnomaly',
  responses: 'data.totalResponses',
  resrate: 'data.responseRate',
  precision: 'data.precision'
};

/**
 * Fetches all anomaly data for found anomalies - downloads all 'pages' of data from server
 * in order to handle sorting/filtering on the entire set locally. Start/end date are not used here.
 * @param {Array} anomalyIds - list of all found anomaly ids
 * @return {Ember.RSVP promise}
 */
const fetchAppAnomalies = (alertList) => {
  const alertPromises = [];
  const startDate = buildDateEod(3, 'month').toISOString();
  const endDate = buildDateEod(1, 'day').toISOString();
  const tuneParams = `start=${startDate}&end=${endDate}`;

  alertList.forEach((alert) => {
    let { name, id } = alert;
    let getAlertPerfHash = {
      id,
      name,
      data: fetch(`/detection-job/eval/filter/${alert.id}?${tuneParams}`).then(checkStatus)
    };
    alertPromises.push(hash(getAlertPerfHash));
  });

  return allSettled(alertPromises);
};

/**
 * Associate each anomaly with an application name. This is done by:
 * 1) For each existing application, find all alert groups associated with it
 * 2) Add the app name to each of the group's function Ids (making sure we don't duplicate an Id)
 * @param {Array} allApps - list of all applications
 * @param {Array} validGroups - all alert groups that are active
 * @returns {Array} appBucket
 */
const fillAppBuckets = (allApps, validGroups) => {
  let appBucket = [];

  allApps.forEach((app) => {
    let associatedGroups = validGroups.filter(group => group.application.toLowerCase().includes(app.application));
    if (associatedGroups.length) {
      let uniqueIds = Array.from(new Set([].concat(...associatedGroups.map(group => group.emailConfig.functionIds))));
      if (demoMode) {
        uniqueIds = uniqueIds.slice(0, 1);
      }
      if (uniqueIds.length) {
        uniqueIds.forEach((id) => {
          appBucket.push({ name: app.application, id });
        });
      }
    }
  });

  return appBucket;
};

/**
 * Simply average the given array of numbers
 * @param {Array} values - all values to average
 * @returns {Number} average value
 */
const average = (values) => {
  return (values.reduce((total, amount) => amount += total))/values.length;
};

/**
 * Check whether there are any fetch promise failures
 * @param {Array} data - all settled RSVP promises
 * @returns {Boolean} true if a failure was found
 */
const isPromiseRejected = (data) => {
  return data.map(obj => obj.state).some(state => state === 'rejected');
};

/**
 * Calculate the standard deviation, or variance in the given array
 * @param {Array} values - all values to average
 * @returns {Number} standard deviation
 */
const standardDeviation = (values) => {
  let avg = average(values);

  let squareDiffs = values.map((value) => {
    let diff = value - avg;
    let sqrDiff = diff * diff;
    return sqrDiff;
  });

  let avgSquareDiff = average(squareDiffs);
  let stdDev = Math.sqrt(avgSquareDiff);
  return stdDev;
};

/**
 * Derive the response or precision rate for a set of anomalies
 * @param {Array} anomalies - all anomalies in a given application
 * @param {Number} subset - the target subset (true anomalies, false, etc)
 * @returns {Number} a percentage
 */
const calculateRate = (anomalies, subset) => {
  let percentage = 0;
  if (anomalies && subset) {
    percentage = (subset * 100) / anomalies;
  }
  return Number(percentage.toFixed());
};

export default Route.extend({

  actions: {
    /**
    * Refresh route's model.
    * @method refreshModel
    * @return {undefined}
    */
    refreshModel() {
      this.refresh();
    }
  },

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model() {
    return RSVP.hash({
      // Fetch all alert group configurations
      configGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      applications: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    });
  },

  afterModel(model) {
    this._super(model);

    const activeGroups = model.configGroups.filterBy('active');
    const groupsWithAppName = activeGroups.filter(group => isPresent(group.application));
    const groupsWithAlertId = groupsWithAppName.filter(group => group.emailConfig.functionIds.length > 0);
    const idsByApplication = fillAppBuckets(model.applications, groupsWithAlertId);
    Object.assign(model, { idsByApplication });
  },

  setupController(controller, model) {
    this._super(controller, model);

    // Display loading banner
    controller.set('isDataLoading', true);

    // Get perf data for each alert and assign it to the model
    fetchAppAnomalies(model.idsByApplication)
      .then((richFunctionObjects) => {
        // Catch any rejected promises
        if (isPromiseRejected(richFunctionObjects)) {
          throw new Error('API error');
        }

        const newFunctionObjects = richFunctionObjects.map(obj => obj.value);
        const availableGroups = Array.from(new Set(newFunctionObjects.map(alertObj => alertObj.name)));
        const roundable = ['totalAlerts', 'totalResponses', 'falseAlarm', 'newTrend', 'trueAnomalies', 'userReportAnomaly'];
        let sortMenuGlyph = {};
        let newGroupArr = [];
        let count = 0;

        // Filter down to functions belonging to our active application groups
        availableGroups.forEach((group) => {
          let avgData = {};
          let keyData = {};
          let groupData = newFunctionObjects.filter((alert) => {
            return alert.name === group;
          });

          // Get array of keys from first record
          let metricKeys = Object.keys(groupData[0].data);
          let getTotalValue = (key) => getWithDefault(avgData, key, 0);
          count++;

          // Look at each anomaly's perf object keys. For our "roundable" fields, get derived data
          metricKeys.forEach((key) => {
            let isRawValue = roundable.includes(key);
            let allValues = groupData.map(group => group.data[key]);
            let allNumeric = allValues.every(val => !Number.isNaN(Number(val)));
            let total = allValues.reduce((total, amount) => amount += total);
            avgData[key] = {};

            if (allNumeric && isRawValue) {
              let avg = total/allValues.length;
              avgData[key].avg = Math.round(avg);
              avgData[key].tot = total;
              avgData[key].max = Math.max(...allValues);
              avgData[key].min = Math.min(...allValues);
              avgData[key].std = standardDeviation(allValues).toFixed(2);
              avgData[key].name = group;
            } else {
              let avg = total/(allValues.filter(Number)).length;
              avgData[key].avg = !Number.isNaN(Number(avg)) ? `${(avg * 100).toFixed(1)}` : 'N/A';
              avgData[key].tot = 'N/A';
            }
            avgData[key].values = allValues;
          });

          // Gather totals and make custom calculations
          let pTrue = getTotalValue('trueAnomalies.tot');
          let pNew = getTotalValue('newTrend.tot');
          let pFalse = getTotalValue('falseAlarm.tot');
          let rResponses = getTotalValue('totalResponses.tot');
          let rTotal = getTotalValue('totalAlerts.tot');
          let precisionTotal = pTrue + pNew + pFalse;
          avgData['responseRate'] = avgData['responseRate'] ? calculateRate(rTotal, rResponses) : 'N/A';
          avgData['precision'] = avgData['precision'] ? calculateRate(precisionTotal, pTrue + pNew) : 'N/A';

          // Add perf data to application groups array
          newGroupArr.push({
            name: demoMode ? `group ${count}` : group,
            data: avgData,
            alerts: groupData.length
          });
        });

        // Initialize glyph icons for each table column
        for (var key in sortMap) {
          sortMenuGlyph[key] = 'down';
        }

        // Pass perf data and state to controller
        controller.setProperties({
          sortMap,
          sortMenuGlyph,
          viewTotals: true,
          isDataLoading: false,
          isDataLoadingError: false,
          perfDataByApplication: newGroupArr
        });

      })
      .catch(() => {
        controller.set('isDataLoadingError', true);
      });
  }
});
