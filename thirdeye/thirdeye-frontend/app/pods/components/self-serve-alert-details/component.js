/**
 * This component displays an alert summary section for users to see alert properties at a glance.
 * Initially used for consistency in both alert index page and single alert details page.
 * We use slightly different sub class names for positioning based on use case.
 * @module components/self-serve-alert-details
 * @property {Object} alertData    - alert properties
 * @property {Boolean} isLoadError - was there an error loading the data
 * @property {String} displayMode  - is the use case part of a list or standalone? 'list' || 'single'
 * @example
    {{#self-serve-alert-details
      alertData=alertData
      isLoadError=false
      displayMode="list"
    }}
      ...additional case-specific header content
    {{/self-serve-alert-details}}
 * @exports self-serve-alert-details
 * @author smcclung
 */

import Component from '@ember/component';
import { setProperties, get, computed } from '@ember/object';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import moment from 'moment';

/* eslint-disable ember/avoid-leaking-state-in-ember-objects */
export default Component.extend({
  valueClassSuffix: '',
  modeSubClass: 'list',

  init() {
    this._super(...arguments);
    const mode = get(this, 'displayMode');
    if (mode === 'single') {
      setProperties(this, {
        valueClassSuffix: '-solo',
        modeSubClass: 'solo'
      });
    }
  },

  // eslint-disable-next-line ember/avoid-leaking-state-in-ember-objects
  labelMap: {
    GOOD: '--good',
    MODERATE: '--average',
    BAD: '--poor',
    UNKNOWN: '--unknown'
  },
  statusMap: {
    GOOD: 'Good',
    MODERATE: 'Average',
    BAD: 'Poor',
    UNKNOWN: 'Unknown'
  },

  /**
   * Changes the color of text in Detection Health
   * @type {String}
   */
  tasks: computed('health', function () {
    const health = get(this, 'health');
    const tasks = [];
    const taskTypes = ['not run', 'succeeded', 'failed', 'timeout'];
    const numTasks = [0, 0, 0, 0];
    if (health && health.detectionTaskStatus && typeof health.detectionTaskStatus === 'object') {
      if (typeof health.detectionTaskStatus.taskCounts === 'object') {
        const taskCounts = health.detectionTaskStatus.taskCounts;
        const keys = ['WAITING', 'COMPLETED', 'FAILED', 'TIMEOUT'];
        for (let i = 0; i < keys.length; i++) {
          numTasks[i] = taskCounts[keys[i]];
        }
      }
    }
    const taskLabels = [null, '--good', '--poor', '--average'];
    for (let i = 0; i < taskTypes.length; i++) {
      const task = {
        title: taskTypes[i],
        number: numTasks[i],
        label: taskLabels[i]
      };
      tasks.push(task);
    }
    return tasks;
  }),

  /**
   * Maps backend values for anomaly coverage status to UI values
   * @type {String}
   */
  anomalyCoverage: computed('health', function () {
    const { health, statusMap, labelMap } = this.getProperties('health', 'statusMap', 'labelMap');
    const info = {};
    if (health && health.anomalyCoverageStatus && typeof health.anomalyCoverageStatus === 'object') {
      info.ratio = floatToPercent(health.anomalyCoverageStatus.anomalyCoverageRatio);
      info.status = statusMap[health.anomalyCoverageStatus.healthStatus];
      info.label = labelMap[health.anomalyCoverageStatus.healthStatus];
    }
    return info;
  }),

  /**
   * Maps backend values for detection status to UI values
   * @type {String}
   */
  detection: computed('health', function () {
    const { health, statusMap, labelMap } = this.getProperties('health', 'statusMap', 'labelMap');
    const info = {};
    if (health && health.detectionTaskStatus && typeof health.detectionTaskStatus === 'object') {
      info.status = statusMap[health.detectionTaskStatus.healthStatus];
      info.label = labelMap[health.detectionTaskStatus.healthStatus];
    }
    return info;
  }),

  /**
   * Formats execution time for UI display
   * @type {String}
   */
  executionTime: computed('health', function () {
    const lastTaskExecutionTimestamp = get(this, 'health').detectionTaskStatus.lastTaskExecutionTime;
    if (lastTaskExecutionTimestamp > 0) {
      const executionDateTime = new Date(lastTaskExecutionTimestamp);
      return (
        executionDateTime.toDateString() +
        ', ' +
        executionDateTime.toLocaleTimeString() +
        ' (' +
        moment().tz(moment.tz.guess()).format('z') +
        ')'
      );
    }
    return '-';
  }),

  /**
   * Maps backend values for overall status to UI values
   * @type {String}
   */
  overall: computed('health', function () {
    const { health, statusMap, labelMap } = this.getProperties('health', 'statusMap', 'labelMap');
    const info = {};
    if (health && typeof health === 'object') {
      info.status = statusMap[health.overallHealth];
      info.label = labelMap[health.overallHealth];
    }
    return info;
  }),

  /**
   * generates regression details for rule selected or first rule if no rule selected
   * @type {Object}
   */
  regressionInfo: computed('health', 'selectedRule', function () {
    const { health, labelMap, selectedRule, statusMap } = this.getProperties(
      'health',
      'labelMap',
      'selectedRule',
      'statusMap'
    );
    const info = {};
    info.mape = floatToPercent(NaN); // set default to Nan
    let rule = selectedRule ? selectedRule.detectorName : null;
    info.status = 'Unknown';
    // 3 possibilities: selectedRule, no selectedRule and rules available, no rules available
    if (health && health.regressionStatus && typeof health.regressionStatus === 'object') {
      const regressionStatus = health.regressionStatus;
      info.status = statusMap[regressionStatus.healthStatus]; // default status will be overall regression status
      info.label = labelMap[regressionStatus.healthStatus];
      if (
        typeof regressionStatus.detectorMapes === 'object' &&
        typeof regressionStatus.detectorHealthStatus === 'object' &&
        Object.keys(regressionStatus.detectorMapes).length != 0
      ) {
        // There is a selectedRule
        if (rule) {
          info.mape = floatToPercent(regressionStatus.detectorMapes[rule]);
          info.status = statusMap[regressionStatus.detectorHealthStatus[rule]];
          info.label = labelMap[regressionStatus.detectorHealthStatus[rule]];
          info.rule = selectedRule.name;
        } else {
          // There is no selectedRule
          let mapes = [];
          let ruleHealth = [];
          if (typeof regressionStatus.detectorMapes === 'object') {
            mapes = Object.keys(regressionStatus.detectorMapes);
          }
          if (typeof regressionStatus.detectorHealthStatus === 'object') {
            ruleHealth = Object.keys(regressionStatus.detectorHealthStatus);
          }
          // There are rules available
          if (mapes.length > 0 && ruleHealth.length > 0) {
            info.mape = floatToPercent(regressionStatus.detectorMapes[mapes[0]]);
            info.status = statusMap[regressionStatus.detectorHealthStatus[mapes[0]]];
            info.label = labelMap[regressionStatus.detectorHealthStatus[mapes[0]]];
            info.rule = mapes[0].split(':')[0];
          }
        }
      }
    }
    return info;
  })
});
