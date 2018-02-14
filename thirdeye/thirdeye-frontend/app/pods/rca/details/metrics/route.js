import { later } from '@ember/runloop';
import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import moment from 'moment';
import { Actions } from 'thirdeye-frontend/actions/metrics';

export default Route.extend({
  redux: service(),

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
      analysisStart,
      analysisEnd,
      displayStart,
      displayEnd
    } = transition.queryParams;

    const queryParams  = Object.assign(defaultQueryParams, transition.queryParams);
    const metricParams = Object.assign({}, params, queryParams);

    if (analysisStart && analysisEnd) {
      redux.dispatch(Actions.updateDates(
        Number(analysisStart),
        Number(analysisEnd)
      ));
    }

    redux.dispatch(Actions.setPrimaryMetric(metricParams))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricIds(res)))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricData(res)))
      .then((res) => redux.dispatch(Actions.fetchRegions(res)))
      .catch(res => res);

    return {
      analysisStart,
      analysisEnd,
      displayStart,
      displayEnd
    };
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      analysisStart,
      analysisEnd,
      displayStart,
      displayEnd
    } = model;

    controller.setProperties({
      analysisStart: Number(analysisStart),
      analysisEnd: Number(analysisEnd),
      displayStart: Number(displayStart),
      displayEnd: Number(displayEnd)
    });
  },

  actions: {
    // Dispatches a redux action on query param change
    queryParamsDidChange(changedParams, oldParams) {
      const redux = this.get('redux');
      let {
        analysisStart: start,
        analysisEnd: end,
        displayStart,
        displayEnd
      } = changedParams;

      const params = Object.keys(changedParams || {});
      const controller = this.controller;

      if (params.length) {
        redux.dispatch(Actions.loading());
        if (start || end) {
          start = start || oldParams.analysisStart;
          end = end || oldParams.analysisEnd;

          later(() => {
            redux.dispatch(Actions.updateDates(
              Number(start),
              Number(end)
            ));
          });
        }

        if (controller && displayStart) {
          controller.set('displayStart', Number(displayStart));
        }

        if (controller && displayEnd) {
          controller.set('displayEnd', Number(displayEnd));
        }

        if (controller && start) {
          controller.set('analysisStart', Number(start));
        }

        if (controller && end) {
          controller.set('analysisEnd', Number(end));
        }

        later(() => {
          redux.dispatch(Actions.loaded());
        });
      }

      this._super(...arguments);

      return true;
    }
  }

});
