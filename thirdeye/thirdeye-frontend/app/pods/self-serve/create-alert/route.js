/**
 * Handles the 'create alert' route nested in the 'manage' route.
 * @module self-serve/create/route
 * @exports alert create model
 */
import fetch from 'fetch';
import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import { selfServeApiOnboard } from 'thirdeye-frontend/utils/api/self-serve';
import { postProps, checkStatus } from 'thirdeye-frontend/utils/utils';
import { inject as service } from '@ember/service';
import { yamlAlertSettings } from 'thirdeye-frontend/utils/constants';

const CREATE_GROUP_TEXT = 'Create a new subscription group';

export default Route.extend({
  anomaliesApiService: service('services/api/anomalies'),
  session: service(),
  store: service('store'),

  /**
   * Model hook for the create alert route.
   * @method model
   * @return {Object}
   */
  async model(params, transition) {
    const debug = transition.state.queryParams.debug || '';
    const applications = await this.get('anomaliesApiService').queryApplications(); // Get all applicatons available
    const subscriptionGroups = await this.get('anomaliesApiService').querySubscriptionGroups(); // Get all subscription groups available

    return hash({
      subscriptionGroups,
      applications,
      debug
    });
  },

  setupController(controller, model) {
    const createGroup = {
      name: CREATE_GROUP_TEXT,
      id: 'n/a',
      yaml: yamlAlertSettings
    };
    const moddedArray = [createGroup];
    // get applications and map to array of vanilla objects
    const applicationNames = this.get('store')
      .peekAll('application')
      .sortBy('application')
      .map(app => {
        return {
          application: app.get('application')
        };
      });
    // get subscription groups and map to array of vanilla objects
    const subscriptionGroups = this.get('store')
      .peekAll('subscription-groups')
      .sortBy('name')
      .filter(group => (group.get('active') && group.get('yaml')))
      .map(group => {
        return {
          name: group.get('name'),
          id: group.get('id'),
          yaml: group.get('yaml'),
          application: group.get('application')
        };
      });
    const subscriptionGroupNamesDisplay = [...moddedArray, ...subscriptionGroups];
    let subscriptionYaml = yamlAlertSettings;
    let groupName = createGroup;
    if (subscriptionGroupNamesDisplay && Array.isArray(subscriptionGroupNamesDisplay) && subscriptionGroupNamesDisplay.length > 0) {
      const firstGroup = subscriptionGroupNamesDisplay[0];
      subscriptionYaml = firstGroup.yaml;
      groupName = firstGroup;
    }
    controller.setProperties({
      subscriptionGroupNames: model.subscriptionGroups,
      subscriptionGroupNamesDisplay,
      allAlertsConfigGroups: subscriptionGroups,
      groupName,
      subscriptionYaml,
      applicationNames,
      model
    });
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
      const { payload } = data;
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
        .catch(() => {
          // Error state will be handled on alert page
          this.jumpToAlertPage(-1, jobName);
        });
    }
  }

});
