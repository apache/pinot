/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import fetch from 'fetch';
import RSVP from 'rsvp';
import moment from 'moment';
import Route from '@ember/routing/route';
import { task, timeout } from 'ember-concurrency';
import {
  selfServeApiCommon,
  selfServeApiOnboard
} from 'thirdeye-frontend/utils/api/self-serve';
import { postProps, checkStatus } from 'thirdeye-frontend/utils/utils';
import { inject as service } from '@ember/service';

let onboardStartTime = {};

export default Route.extend({
  session: service(),
  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  model(params, transition) {
    return RSVP.hash({
      allConfigGroups: fetch(selfServeApiCommon.allConfigGroups).then(checkStatus),
      allAppNames: fetch(selfServeApiCommon.allApplications).then(checkStatus)
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
  jumpToAlertPage(jobId, functionName) {
    const queryParams = { jobId, functionName };
    this.transitionTo('manage.alert', jobId, { queryParams });
  },

  /**
   * Send a DELETE request to the function delete endpoint.
   * @method removeThirdEyeFunction
   * @param {Number} functionId - The id of the alert to remove
   * @return {Promise}
   */
  removeThirdEyeFunction(functionId) {
    const postProps = {
      method: 'delete',
      headers: { 'content-type': 'text/plain' }
    };
    return fetch(selfServeApiOnboard.deleteAlert(functionId), postProps).then(checkStatus);
  },


  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    },
    error() {
      // The `error` hook is also provided the failed
      // `transition`, which can be stored and later
      // `.retry()`d if desired.
      return true;
    },

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
      const { ignore, payload } = data;
      const jobName = payload.functionName;

      fetch(selfServeApiOnboard.updateAlert(jobName), postProps(payload))
        .then(checkStatus)
        .then((result) => {
          if (result.jobStatus && result.jobStatus.toLowerCase() === 'failed') {
            throw new Error(result);
          } else {
            // alert function is created. Redirect to alert page.
            this.controller.set('isProcessingForm', false);
            this.jumpToAlertPage(result.jobId, null);
          }
        })
        .catch((err) => {
          // Error state will be handled on alert page
          this.jumpToAlertPage(-1, jobName);
        });
    }
  }

});
