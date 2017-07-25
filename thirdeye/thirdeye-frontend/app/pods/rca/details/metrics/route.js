import Ember from 'ember';
import moment from 'moment';
import { Actions as MetricsActions } from 'thirdeye-frontend/actions/metrics';

export default Ember.Route.extend({
  redux: Ember.inject.service(),

  model(params, transition) {
    const { metricId } = transition.params['rca.details'];
    const redux = this.get('redux');
    if (!metricId) { return; }

    const defaultQueryParams = {
      startDate: moment().subtract(1, 'day').endOf('day'),
      endDate: moment().subtract(1, 'week').endOf('day'),
      granularity: 'DAYS',
      filters: JSON.stringify({}),
      primaryMetricId: metricId
    };

    const queryParams  = Object.assign(defaultQueryParams, transition.queryParams);
    const metricParams = Object.assign({}, params, queryParams);

    redux.dispatch(MetricsActions.setPrimaryMetric(metricParams))
      .then((res) => redux.dispatch(MetricsActions.fetchRelatedMetricIds(res)))
      .then((res) => redux.dispatch(MetricsActions.fetchRelatedMetricData(res)))
      .then((res) => redux.dispatch(MetricsActions.fetchRegions(res)))
      .catch(res => res);
  }

});
