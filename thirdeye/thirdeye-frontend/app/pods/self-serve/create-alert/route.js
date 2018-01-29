/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import Ember from 'ember';
import fetch from 'fetch';
import RSVP from 'rsvp';
import Route from '@ember/routing/route';
import { postProps, checkStatus } from 'thirdeye-frontend/helpers/utils';

let requestCanContinue = true;
let onboardStartTime = {};

export default Route.extend({
  queryParams: {
    newUx: {
      refreshModel: false,
      replace: true
    }
  },

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model(params, transition) {
    return RSVP.hash({
      // Fetch all alert group configurations
      isNewUx: transition.queryParams.newUx || false,
      allConfigGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      allAppNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
    });
  },

  /**
   * Model hook for the create alert route.
   * @method resetController
   * @param {Object} controller - active controller
   * @param {Boolean} isExiting - exit status
   * @param {Object} transition - transition obj
   * @return {undefined}
   */
  resetController(controller, isExiting) {
    this._super(...arguments);
    if (isExiting) {
      controller.clearAll();
      requestCanContinue = false;
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
    this.transitionTo('manage.alert', alertId, { queryParams: { jobId, functionName }});
  },

  /**
   * Pings the job-info endpoint to check status of an ongoing replay job.
   * If there is no progress after a set time, we display an error message.
   * @method checkReplayStatus
   * @param {Number} jobId - the id for the newly triggered replay job
   * @return {undefined}
   */
  checkJobCreateStatus(jobId, functionName) {
    const checkStatusUrl = `/detection-onboard/get-status?jobId=${jobId}`;
    const alertByNameUrl = `/data/autocomplete/functionByName?name=${functionName}`;

    // In replay status check, continue to display "pending" banner unless we have success.
    fetch(checkStatusUrl).then(checkStatus)
      .then((jobStatus) => {
        const createStatusObj = _.has(jobStatus, 'taskStatuses') ? jobStatus.taskStatuses.find(status => status.taskName === 'FunctionAlertCreation') : null;
        const isCreateComplete = createStatusObj ? createStatusObj.taskStatus.toLowerCase() === 'completed' : false;
        // stop trying if more than 10 seconds has elapsed - call job creation failed.
        let requestCanContinue = Number(moment.duration(moment().diff(onboardStartTime)).asSeconds().toFixed(0)) > 10;

        if (isCreateComplete) {
          fetch(alertByNameUrl).then(checkStatus)
            .then((newAlert) => {
              this.controller.set('isProcessingForm', false);
              const isPresent = Ember.isArray(newAlert) && newAlert.length === 1;
              const alertId = isPresent ? newAlert[0].id : 0;
              this.jumpToAlertPage(alertId, jobId, functionName);
            });
        } else if (requestCanContinue) {
          Ember.run.later(() => {
            this.checkJobCreateStatus(jobId, functionName);
          }, 3000);
        } else {
          // too much time has passed. Show create failure
          this.controller.set('isProcessingForm', false);
          this.jumpToAlertPage(0, 0, functionName);
        }
      })
      .catch((err) => {
        this.jumpToAlertPage(0, 0, functionName);
      });
  },

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
      const onboardUrl = `/detection-onboard/create-job?jobName=${data.jobName}&payload=${encodeURIComponent(data.payload)}`;
      const newName = JSON.parse(data.payload).functionName;
      let onboardStartTime = moment();

      fetch(onboardUrl, postProps('')).then(checkStatus)
        .then((result) => {
          this.checkJobCreateStatus(result.jobId, newName);
        })
        .catch((err) => {
          // Error state will be handled on alert page
          this.jumpToAlertPage(0, 0, newName);
        });
    }
  }

});
