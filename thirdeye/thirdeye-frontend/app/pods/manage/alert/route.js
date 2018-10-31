/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import RSVP from 'rsvp';
import fetch from 'fetch';
import moment from 'moment';
import { later } from '@ember/runloop';
import { isPresent } from "@ember/utils";
import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import config from 'thirdeye-frontend/config/environment';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';

// Setup for query param behavior
const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Route.extend({
  queryParams: {
    jobId: queryParamsConfig
  },

  durationCache: service('services/duration'),
  session: service(),

  beforeModel(transition) {
    const id = transition.params['manage.alert'].alert_id;
    const { jobId, functionName } = transition.queryParams;
    const duration = '3m';
    const startDate = buildDateEod(3, 'month').valueOf();
    const endDate = moment().utc().valueOf();

    // Enter default 'explore' route with defaults loaded in URI
    // An alert Id of 0 means there is an alert creation error to display
    if (transition.targetName === 'manage.alert.index' && Number(id) !== -1) {
      this.transitionTo('manage.alert.explore', id, { queryParams: {
        duration,
        startDate,
        endDate,
        functionName: null,
        jobId
      }});

      // Save duration to service object for session availability
      this.get('durationCache').setDuration({ duration, startDate, endDate });
    }
  },

  model(params, transition) {
    const { alert_id: id, jobId, functionName } = params;
    if (!id) { return; }

    // Fetch all the basic alert data needed in manage.alert subroutes
    // Apply calls from go/te-ss-alert-flow-api
    return RSVP.hash({
      id,
      jobId,
      functionName: functionName || 'Unknown',
      isLoadError: Number(id) === -1,
      destination: transition.targetName,
      alertData: fetch(selfServeApiCommon.alertById(id)).then(checkStatus),
      email: fetch(selfServeApiCommon.configGroupByAlertId(id)).then(checkStatus),
      allConfigGroups: fetch(selfServeApiCommon.allConfigGroups).then(checkStatus),
      allAppNames: fetch(selfServeApiCommon.allApplications).then(checkStatus)
    });
  },

  resetController(controller, isExiting) {
    this._super(...arguments);
    if (isExiting) {
      controller.set('alertData', {});
    }
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      id,
      alertData,
      pathInfo,
      jobId,
      isLoadError,
      functionName,
      destination,
      allConfigGroups
    } = model;

    const newAlertData = !alertData ? {} : alertData;
    let errorText = '';

    // Itereate through config groups to enhance all alerts with extra properties (group name, application)
    allConfigGroups.forEach((config) => {
      let groupFunctionIds = config.emailConfig && config.emailConfig.functionIds ? config.emailConfig.functionIds : [];
      let foundMatch = groupFunctionIds.find(funcId => funcId === Number(id));
      if (foundMatch) {
        Object.assign(newAlertData, {
          application: config.application,
          group: config.name
        });
      }
    });

    const isEditModeActive = destination.includes('edit') || destination.includes('tune');
    const pattern = newAlertData.alertFilter ? newAlertData.alertFilter.pattern : 'N/A';
    const granularity = newAlertData.bucketSize && newAlertData.bucketUnit ? `${newAlertData.bucketSize}_${newAlertData.bucketUnit}` : 'N/A';
    Object.assign(newAlertData, { pattern, granularity });

    // We do not have a valid alertId. Set error state.
    if (isLoadError) {
      Object.assign(newAlertData, { functionName, isActive: false });
      errorText = `${functionName.toUpperCase()} has failed to create. Please try again or email ${config.email}`;
    }

    controller.setProperties({
      id,
      pathInfo,
      errorText,
      isLoadError,
      isEditModeActive,
      alertData: newAlertData,
      isTransitionDone: true,
      isReplayPending: isPresent(jobId)
    });
  },

  actions: {
    /**
     * Set loader on start of transition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }

      this.controller.set('isTransitionDone', false);
      if (transition.targetName === 'manage.alert.index') {
        this.refresh();
      }
    },

    /**
     * Once transition is complete, remove loader
     */
    didTransition() {
      this.controller.set('isTransitionDone', true);
      // This is needed in order to update the links in this parent route,
      // giving the "currentRouteName" time to resolve
      later(this, () => {
        if (this.router.currentRouteName.includes('explore')) {
          this.controller.set('isEditModeActive', false);
        }
      });
    },

    // // Sub-route errors will bubble up to this
    error() {
      if (this.controller) {
        this.controller.set('isLoadError', true);
      }
      return true;//pass up stream
    }
  }

});
