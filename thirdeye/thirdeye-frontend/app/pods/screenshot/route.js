import Route from '@ember/routing/route';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import RSVP from "rsvp";
import UnauthenticatedRouteMixin from 'ember-simple-auth/mixins/unauthenticated-route-mixin';

export default Route.extend(UnauthenticatedRouteMixin, {
  model(params) {
    const { anomaly_id: id} = params;
    const anomalyUrl = `/dashboard/anomalies/view/${id}`;

    return fetch(anomalyUrl)
      .then(checkStatus)
      .then(res => {
        const predictedUrl = `/detection/predicted-baseline/${id}?start=${res.startTime}&end=${res.endTime}&padding=true`;
        const timeseriesHash = {
          predicted: fetch(predictedUrl).then(res => checkStatus(res, 'get', true)),
          anomalyData: res
        };
        return RSVP.hash(timeseriesHash);
      });
  },

  /**
   * Sets current, predicted, and anomalyData
   * @return {undefined}
   */
  setupController(controller, model) {
    this._super(...arguments);
    controller.setProperties({
      current: model.predicted,
      predicted: model.predicted,
      anomalyData: model.anomalyData
    });
  }
});
