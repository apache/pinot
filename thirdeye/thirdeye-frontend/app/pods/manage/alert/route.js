/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import RSVP from 'rsvp';
import fetch from 'fetch';
import { isPresent } from "@ember/utils";
import Route from '@ember/routing/route';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';

// Setup for query param behavior
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

export default Route.extend({
  queryParams: {
    jobId: queryParamsConfig
  },

  beforeModel(transition) {
    const id = transition.params['manage.alert'].alertId;
    const { jobId, functionName } = transition.queryParams;
    const durationDefault = '3m';
    const startDateDefault = buildDateEod(3, 'month').valueOf();
    const endDateDefault = buildDateEod(1, 'day');

    // Enter default 'explore' route with defaults loaded in URI
    // An alert Id of 0 means there is an alert creation error to display
    if (transition.targetName === 'manage.alert.index' && Number(id) !== -1) {
      this.transitionTo('manage.alert.explore', id, { queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault,
        functionName: null,
        jobId
      }});
    }
  },

  model(params, transition) {
    const { alertId: id, jobId, functionName } = params;
    if (!id) { return; }

    // Fetch all the basic alert data needed in manage.alert subroutes
    // Apply calls from go/te-ss-alert-flow-api
    return RSVP.hash({
      id,
      jobId,
      functionName: functionName || 'Unknown',
      isLoadError: Number(id) === -1,
      destination: transition.targetName,
      alertData: fetch(`/onboard/function/${id}`).then(checkStatus),
      email: fetch(`/thirdeye/email/function/${id}`).then(checkStatus),
      allConfigGroups: fetch('/thirdeye/entity/ALERT_CONFIG').then(res => res.json()),
      allAppNames: fetch('/thirdeye/entity/APPLICATION').then(res => res.json())
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
      errorText = `We were not able to confirm alert creation: alert name ${functionName.toUpperCase()} not found in DB`;
    }

    controller.setProperties({
      id,
      pathInfo,
      errorText,
      isLoadError,
      isEditModeActive,
      alertData: newAlertData,
      isOverViewModeActive: !isEditModeActive,
      isReplayPending: isPresent(jobId)
    });
  },

  actions: {
    willTransition(transition) {
      if (transition.targetName === 'manage.alert.index') {
        this.refresh();
      }
    },

    // Sub-route errors will bubble up to this
    error(error, transition) {
      this.controller.set('isLoadError', true);
    }
  }

});
