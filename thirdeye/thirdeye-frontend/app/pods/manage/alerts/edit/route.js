import Ember from 'ember';
import fetch from 'fetch';
import moment from 'moment';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

export default Ember.Route.extend({
  model(params) {
    const { alertId: id } = params;
    if (!id) { return; }

    const url = `onboard/function/${id}`;
    return fetch(url).then(checkStatus);
  },

  afterModel(model) {

    const {
      metric: metricName,
      collection: dataset
     } = model;

    const granularity = model.frequency.unit;
    let id = '';

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
    .then((metrics) => {
      const metric = metrics.pop();
      id = metric.id;
      return fetch(`/data/maxDataTime/metricId/${id}`).then(checkStatus);
    })
    .then((maxTime) => {
      const currentEnd = moment(maxTime).isValid()
        ? moment(maxTime).valueOf()
        : moment().subtract(1, 'day').endOf('day').valueOf();
      const filters = '';
      const dimension = 'All';
      const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
      const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
      const baselineEnd = moment(currentEnd).subtract(1, 'week');
      const url =  `/timeseries/compare/${id}/${currentStart}/${currentEnd}/` +
        `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}` +
        `&filters=${encodeURIComponent(filters)}`;
      return fetch(url).then(checkStatus);
    })
    .then((metricData) => {
      Object.assign(model, { metricData });
    });
  }
});
