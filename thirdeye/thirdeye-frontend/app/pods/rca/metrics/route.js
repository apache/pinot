import Ember from 'ember';
import moment from 'moment';
import { Actions as MetricsActions } from 'thirdeye-frontend/actions/metrics';


export default Ember.Route.extend({
  queryParams: {
    startDate: {
      refreshModel: true
    },
    endDate: {
      refreshModel: true
    },
    granularity: {
      refreshModel: true
    },
    filters: {
      refreshModel: true
    },
    compareMode: {
      refreshModel: true
    }
  },

  redux: Ember.inject.service(),

  /**
   * Massages Query Params from URL and dispatch redux actions
   */
  model(params, transition) {
    if (!params.id) { return; }

    const redux = this.get('redux');
    const defaultQueryParams = {
      startDate: moment().subtract(1, 'day').endOf('day'),
      endDate: moment().subtract(1, 'week').endOf('day'),
      granularity: 'DAYS',
      filters: JSON.stringify({}),
    }
    const queryParams  = Object.assign(defaultQueryParams, transition.queryParams);
    const metricParams = Object.assign({}, params, queryParams)

    redux.dispatch(MetricsActions.setPrimaryMetric(metricParams))
      .then((res) => redux.dispatch(MetricsActions.fetchRelatedMetricIds(res)))
      .then((res) => redux.dispatch(MetricsActions.fetchRegions(res)))
      .then((res) => redux.dispatch(MetricsActions.fetchRelatedMetricData(res)))
      .catch((error) => redux.dispatch(MetricsActions.requestFail(error)));
      
    return {};
  }
});
