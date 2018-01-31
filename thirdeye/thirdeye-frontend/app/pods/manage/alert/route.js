/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import RSVP from 'rsvp';
import fetch from 'fetch';
import Route from '@ember/routing/route';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/utils/utils';

// Setup for query param behavior
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

export default Route.extend({
  queryParams: {
    replayId: queryParamsConfig
  },

  beforeModel(transition) {
    const id = transition.params['manage.alert'].alertId;
    const replayId = transition.queryParams.replayId;
    const durationDefault = '1m';
    const startDateDefault = buildDateEod(1, 'month').valueOf();
    const endDateDefault = buildDateEod(1, 'day');

    // Enter default 'explore' route with defaults loaded in URI
    if (transition.targetName === 'manage.alert.index') {
      this.transitionTo('manage.alert.explore', id, { queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault,
        replayId
      }});
    }
  },

  model(params, transition) {
    const { alertId: id, replayId } = params;
    if (!id) { return; }

    // Fetch all the basic alert data needed in manage.alert subroutes
    // TODO: apply calls from go/te-ss-alert-flow-api (see below)
    return RSVP.hash({
      id,
      replayId,
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
      replayId,
      destination,
      allConfigGroups
    } = model;

    // Itereate through config groups to enhance all alerts with extra properties (group name, application)
    allConfigGroups.forEach((config) => {
      let groupFunctionIds = config.emailConfig && config.emailConfig.functionIds ? config.emailConfig.functionIds : [];
      let foundMatch = groupFunctionIds.find(funcId => funcId === Number(id));
      if (foundMatch) {
        Object.assign(alertData, {
          application: config.application,
          group: config.name
        });
      }
    });

    const isEditModeActive = destination.includes('edit') || destination.includes('tune');
    const pattern = alertData.alertFilter ? alertData.alertFilter.pattern : 'N/A';
    const granularity = alertData.bucketSize && alertData.bucketUnit ? `${alertData.bucketSize}_${alertData.bucketUnit}` : 'N/A';
    Object.assign(alertData, { pattern, granularity });

    controller.setProperties({
      id,
      alertData,
      pathInfo,
      replayId,
      isEditModeActive,
      isOverViewModeActive: !isEditModeActive,
      isReplayPending: Ember.isPresent(replayId)
    });
  },

  actions: {
    willTransition(transition) {
      if (transition.targetName === 'manage.alert.index') {
        this.refresh();
      }
    },

    error(error, transition) {
      this.controller.set('isLoadError', true);
    }
  }

});
