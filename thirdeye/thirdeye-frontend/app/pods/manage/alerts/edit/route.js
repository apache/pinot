import Ember from 'ember';
import fetch from 'fetch';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Route.extend({
  model(params) {
    const { alertId: id } = params;
    if (!id) { return; }

    const url = `onboard/function/${id}`;
    return fetch(url).then(checkStatus);
  },

  afterModel(model) {
    Object.assign(model,
      { metricData: {data: [1, 2, 3, 4] }});

    // fetchMetricData(metricId) {
    //   const promiseHash = {
    //     maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => checkStatus(res, 'get', true)),
    //     granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
    //     filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
    //     dimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res => checkStatus(res, 'get', true))
    //   };
    //   return Ember.RSVP.hash(promiseHash);
    // },

  }
});
