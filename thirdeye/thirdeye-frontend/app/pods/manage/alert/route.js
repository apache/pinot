/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';
import { checkStatus, buildDateEod } from 'thirdeye-frontend/helpers/utils';

// Setup for query param behavior
const queryParamsConfig = {
  refreshModel: true,
  replace: true
};

const replaceConfig = {
  replace: true
};

export default Ember.Route.extend({
  queryParams: {
    replay: replaceConfig,
    replayId: replaceConfig
  },

  beforeModel(transition) {
    const durationDefault = '1m';
    const startDateDefault = buildDateEod(1, 'month').valueOf();
    const endDateDefault = buildDateEod(1, 'day');
    const id = transition.params['manage.alert'].alertId;

    // Enter default 'explore' route with defaults loaded in URI
    if (transition.targetName === 'manage.alert.index') {
      this.transitionTo('manage.alert.explore', id, { queryParams: {
        duration: durationDefault,
        startDate: startDateDefault,
        endDate: endDateDefault
      }});
    }
  },

  model(params) {
    const { alertId: id } = params;
    if (!id) { return; }

    // Fetch all the basic alert data needed in manage.alert subroutes
    // TODO: apply calls from go/te-ss-alert-flow-api (see below)
    return Ember.RSVP.hash({
      id,
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
    const { id, alertData, pathInfo } = model;
    controller.setProperties({ id, alertData, pathInfo });
  },

  actions: {
    willTransition(transition) {
      if (transition.targetName === 'manage.alert.index') {
        this.refresh();
      }
    }
  }

});
