import Ember from 'ember';
import fetch from 'fetch';
import moment from 'moment';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

const parseProps = (filters) => {
  filters = filters || '';

  return filters.split(';')
    .filter(prop => prop)
    .map(prop => prop.split('='))
    .reduce(function (aggr, prop) {
      const [ propName, value ] = prop;
      aggr[propName] = value;

      return aggr;
    }, {});
};



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
      collection: dataset,
      filters,
      bucketUnit: granularity,
      id
     } = model;

    let metricId = '';

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      .then((metrics) => {
        const metric = metrics.pop();
        metricId = metric.id;
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })
      .then((maxTime) => {
        const currentEnd = moment(maxTime).isValid()
          ? moment(maxTime).valueOf()
          : moment().subtract(1, 'day').endOf('day').valueOf();
        const formattedFilters = JSON.stringify(parseProps(filters));
        const dimension = 'All';
        const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
        const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
        const baselineEnd = moment(currentEnd).subtract(1, 'week');
        const url =  `/timeseries/compare/${metricId}/${currentStart}/${currentEnd}/` +
          `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}` +
          `&filters=${encodeURIComponent(formattedFilters)}`;
        return fetch(url).then(checkStatus);
      })
      .then((metricData) => {
        Object.assign(model, { metricData });

        return fetch(`thirdeye/email/functions`).then(checkStatus);
      })
      .then((groupConfigs) => {
        const subscriptionGroups = groupConfigs[id] || [];

        // Back end supports 1-many relationships, however, we currently
        // enforces 1-1 in the front end
        const subscriptionGroup = subscriptionGroups.pop();

        Object.assign(model, { subscriptionGroup });
      });
  },
  actions: {
    refreshModel: function() {
      this.refresh();
    }
  }
});
