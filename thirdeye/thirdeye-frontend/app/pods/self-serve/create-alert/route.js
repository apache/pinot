/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import fetch from 'fetch';
import RSVP from 'rsvp';
import _ from 'lodash';
import moment from 'moment';
import { isArray } from "@ember/array";
import Route from '@ember/routing/route';
import { task, timeout } from 'ember-concurrency';
import { postProps, checkStatus } from 'thirdeye-frontend/utils/utils';

let onboardStartTime = {};

export default Route.extend({

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model(params, transition) {
    return RSVP.hash({
      allConfigGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      allAppNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    });
  },

  /**
   * Model hook for the create alert route.
   * @method resetController
   * @param {Object} controller - active controller
   * @param {Boolean} isExiting - exit status
   * @return {undefined}
   */
  resetController(controller, isExiting) {
    this._super(...arguments);
    if (isExiting) {
      controller.clearAll();
      this.get('checkJobCreateStatus').cancelAll();
    }
  },

  /**
   * Transition to Alert Page with query params related to alert creation job
   * @method jumpToAlertPage
   * @param {Number} alertId - Id of alert just created
   * @param {Number} jobId - Id of alert creation job
   * @param {String} functionName - name of new alert function
   * @return {undefined}
   */
  jumpToAlertPage(alertId, jobId, functionName) {
    const queryParams = { jobId, functionName };
    this.transitionTo('manage.alert', alertId, { queryParams });
  },

  /**
   * Concurrenty task to ping the job-info endpoint to check status of an ongoing replay job.
   * If there is no progress after a set time, we display an error message.
   * @param {Number} jobId - the id for the newly triggered replay job
   * @param {String} functionName - user-provided new function name (used to validate creation)
   * @return {undefined}
   */
  checkJobCreateStatus: task(function * (jobId, functionName, functionId) {
    yield timeout(2000);
    const checkStatusUrl = `/detection-onboard/get-status?jobId=${jobId}`;

    // In replay status check, continue to display "pending" banner unless we have success or create job takes more than 10 seconds.
    return fetch(checkStatusUrl).then(checkStatus)
      .then((jobStatus) => {
        const createStatusObj = _.has(jobStatus, 'taskStatuses') ? jobStatus.taskStatuses.find(status => status.taskName === 'FunctionAlertCreation') : null;
        const isCreateComplete = createStatusObj ? createStatusObj.taskStatus.toLowerCase() === 'completed' : false;
        const secondsSinceOnboardStart = Number(moment.duration(moment().diff(onboardStartTime)).asSeconds().toFixed(0));
        if (isCreateComplete) {
          // alert function is created. Redirect to alert page.
          this.controller.set('isProcessingForm', false);
          this.jumpToAlertPage(functionId, jobId, null);
        } else if (secondsSinceOnboardStart < 20) {
          // alert creation is still pending. check again.
          this.get('checkJobCreateStatus').perform(jobId, functionName, functionId);
        } else {
          // too much time has passed. Show create failure.
          this.controller.set('isProcessingForm', false);
          this.jumpToAlertPage(-1, -1, functionName);
        }
      })
      .catch((err) => {
        // in the event of either call failing, display alert page error state.
        this.jumpToAlertPage(-1, -1, functionName);
      });
  }),

  actions: {
    /**
    * Refresh route's model.
    * @method refreshModel
    */
    refreshModel() {
      this.refresh();
    },

    /**
    * Trigger onboarding sequence starting with alert creation. Once triggered,
    * we must look up the new alert Id as confirmation.
    * @param {Object} data - contains request query params for alert creation job
    * @method triggerReplaySequence
    */
    triggerOnboardingJob(data) {
      const newName = JSON.parse(data.payload).functionName;
      const createAlertUrl = `/function-onboard/create-function?name=${newName}`;
      const updateAlertUrl = `/detection-onboard/create-job?jobName=${data.jobName}&payload=${encodeURIComponent(data.payload)}`;
      let onboardStartTime = moment();
      let newFunctionId = '';

      fetch(createAlertUrl, postProps('')).then(checkStatus)
        .then((result) => {
          newFunctionId = result.id;
          return fetch(updateAlertUrl, postProps('')).then(checkStatus);
        })
        .then((result) => {
          this.get('checkJobCreateStatus').perform(result.jobId, newName, newFunctionId);
        })
        .catch((err) => {
          // Error state will be handled on alert page
          this.jumpToAlertPage(-1, -1, newName);
        });
    }
  }

});
