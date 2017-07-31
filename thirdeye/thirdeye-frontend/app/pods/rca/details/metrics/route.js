import Ember from 'ember';
import moment from 'moment';
import { Actions } from 'thirdeye-frontend/actions/metrics';

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
    const {
      analysisStart: start,
      analysisEnd: end
    } = transition.queryParams;

    const queryParams  = Object.assign(defaultQueryParams, transition.queryParams);
    const metricParams = Object.assign({}, params, queryParams);

    if (start && end) {
      redux.dispatch(Actions.updateDates(
        Number(start),
        Number(end)
      ));
    }

    redux.dispatch(Actions.setPrimaryMetric(metricParams))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricIds(res)))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricData(res)))
      .then((res) => redux.dispatch(Actions.fetchRegions(res)))
      .catch(res => res);
  },

  actions: {
    // Dispatches a redux action on query param change
    queryParamsDidChange(changedParams, oldParams) {
      const redux = this.get('redux');
      let {
        analysisStart: start,
        analysisEnd: end
      } = changedParams;
      const params = Object.keys(changedParams || {});

      if (params.length && (start || end)) {
        start = start || oldParams.analysisStart;
        end = end || oldParams.analysisEnd;

        Ember.run.later(() => {
          redux.dispatch(Actions.updateDates(
            Number(start),
            Number(end)
          ));
        });
      }
      this._super(...arguments);

      return true;
    }
  }

});
