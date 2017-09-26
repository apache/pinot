import Ember from 'ember';
import fetch from 'fetch';
import moment from 'moment';
import RSVP from 'rsvp';
import _ from 'lodash';
import { checkStatus } from 'thirdeye-frontend/helpers/utils';

/**
 * Parses stringified object from payload
 * @param {String} filters
 * @returns {Object}
 */
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
    const alertUrl = `/onboard/function/${id}`;

    return RSVP.hash({
      function: fetch(alertUrl).then(checkStatus)
    });
  },

  afterModel(model) {
    const {
      metric: metricName,
      collection: dataset,
      filters,
      bucketUnit: granularity,
      id
     } = model.function;

    let metricId = '';
    let allGroupNames = [];
    let allGroups = [];

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      .then((metricsByName) => {
        const metric = metricsByName.pop();
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
        const metricDataUrl =  `/timeseries/compare/${metricId}/${currentStart}/${currentEnd}/` +
          `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}` +
          `&filters=${encodeURIComponent(formattedFilters)}`;
        return fetch(metricDataUrl).then(checkStatus);
      })
      .then((metricData) => {
        Object.assign(metricData, { color: 'blue' })
        Object.assign(model, { metricData });
        return fetch(`/thirdeye/entity/ALERT_CONFIG`).then(checkStatus);
      })
      .then((allConfigGroups) => {
        // TODO: confirm dedupe
        const uniqueGroups = _.uniq(allConfigGroups, name);
        Object.assign(model, { allConfigGroups: uniqueGroups });
        return fetch(`/thirdeye/email/function/${id}`).then(checkStatus);
      })
      .then((groupByAlertId) => {
        const originalConfigGroup = groupByAlertId ? groupByAlertId.pop() : null;
        const selectedAppName = originalConfigGroup ? originalConfigGroup.application : null;
        Object.assign(model, { originalConfigGroup, selectedAppName });
        return fetch('/thirdeye/entity/APPLICATION').then(checkStatus);
      })
      .then((allApps) => {
        Object.assign(model, { allApps });
      });
  },

  actions: {
    /**
     * Action called on submission to reload the route's model
     */
    refreshModel: function() {
      this.refresh();
      this.transitionTo('manage.alerts.edit', this.currentModel.function.id);
    }
  }
});
