import Route from '@ember/routing/route';
import moment from 'moment';
import fetch from 'fetch';
import RSVP from 'rsvp';
import _ from 'lodash';
import {
  checkStatus,
  buildDateEod,
  parseProps
} from 'thirdeye-frontend/utils/utils';

export default Route.extend({
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
      id,
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters,
      bucketSize,
      bucketUnit,
      properties: alertProps
    } = model.function;

    let metricId = '';
    let metricDataUrl = '';
    let metricDimensionURl = '';
    let selectedAppName = '';

    // Add a parsed properties array to the model
    const propsArray = alertProps.split(';').map((prop) => {
      const [ name, value ] = prop.split('=');
      return { name, value: decodeURIComponent(value) };
    });
    Object.assign(model, { propsArray });

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      .then((metricsByName) => {
        const metric = metricsByName.pop();
        metricId = metric.id;
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })
      .then((maxTime) => {
        const dimension = exploreDimensions || 'All';
        const currentEnd = moment(maxTime).isValid()
          ? moment(maxTime).valueOf()
          : buildDateEod(1, 'day').valueOf();
        const formattedFilters = JSON.stringify(parseProps(filters));
        // Load less data if granularity is 'minutes'
        const isMinutely = bucketUnit.toLowerCase().includes('minute');
        const duration = isMinutely ? { unit: 2, size: 'week' } : { unit: 1, size: 'month' };
        const currentStart = moment(currentEnd).subtract(duration.unit, duration.size).valueOf();
        const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
        const baselineEnd = moment(currentEnd).subtract(1, 'week');

        // Prepare call for metric graph data
        metricDataUrl =  `/timeseries/compare/${metricId}/${currentStart}/${currentEnd}/` +
          `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=` +
          `${bucketSize + '_' + bucketUnit}&filters=${encodeURIComponent(formattedFilters)}`;

        // Prepare call for dimension graph data
        metricDimensionURl = `/rootcause/query?framework=relatedDimensions&anomalyStart=${currentStart}` +
          `&anomalyEnd=${currentEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}` +
          `&analysisStart=${currentStart}&analysisEnd=${currentEnd}&urns=thirdeye:metric:${metricId}` +
          `&filters=${encodeURIComponent(filters)}`;

        // Fetch graph metric data
        return fetch(metricDataUrl).then(checkStatus);
      })
      .then((metricData) => {
        Object.assign(metricData, { color: 'blue' });
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
        const originalConfigGroup = groupByAlertId.length ? groupByAlertId.pop() : null;
        selectedAppName = originalConfigGroup ? originalConfigGroup.application : null;
        Object.assign(model, { originalConfigGroup, selectedAppName });
        return fetch('/thirdeye/entity/APPLICATION').then(checkStatus);
      })
      .then((allApps) => {
        const selectedApplication = _.find(allApps, function(appsObj) { return appsObj.application === selectedAppName; });
        Object.assign(model, { allApps, selectedApplication });
        if (exploreDimensions) {
          return fetch(metricDimensionURl).then(checkStatus).then((metricDimensions) => {
            Object.assign(model, { metricDimensions });
          });
        }
      })
      .catch((errors) => {
        Object.assign(model, { loadError: true, loadErrorMsg: errors });
      });
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  setupController(controller, model) {
    this._super(controller, model);

    controller.setProperties({
      model,
      metricData: model.metricData,
      alertDimension: model.function.exploreDimensions,
      metricDimensions: model.metricDimensions,
      metricName: model.function.metric,
      granularity: `${model.function.bucketSize}_${model.function.bucketUnit}`,
      alertFilters: model.function.filters,
      alertProps: model.propsArray,
      alertConfigGroups: model.allConfigGroups,
      alertFunctionName: model.function.functionName,
      alertId: model.function.id,
      isActive: model.function.isActive,
      allApplications: model.allApps,
      selectedConfigGroup: model.originalConfigGroup,
      selectedApplication: model.selectedApplication,
      selectedAppName: model.selectedAppName,
      isLoadError: model.loadError,
      loadErrorMessage: model.loadErrorMsg,
      isGraphVisible: true
    });
  },

  actions: {
    /**
     * Action called on submission to reload the route's model
     */
    refreshModel() {
      this.refresh();
    }
  }
});
