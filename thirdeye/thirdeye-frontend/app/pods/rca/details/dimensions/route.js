import { later } from '@ember/runloop';
import Route from '@ember/routing/route';

import { Actions } from 'thirdeye-frontend/actions/dimensions';
import { inject as service } from '@ember/service';

export default Route.extend({
  redux: service(),

  // queryParam unique to the dimension route
  queryParams: {
    dimension: {
      replace: true,
      refreshModel: true
    }
  },

  model(params, transition) {
    const redux = this.get('redux');
    const { metricId } = transition.params['rca.details'];
    const {
      analysisStart: initStart,
      analysisEnd: initEnd
    } = this.modelFor('rca.details');

    const {
      dimension = 'All',
      analysisStart,
      analysisEnd,
      displayStart,
      displayEnd
    } = transition.queryParams;

    const start = analysisStart || initStart;
    const end = analysisEnd || initEnd;

    if (!metricId) { return; }

    redux.dispatch(Actions.loading());
    redux.dispatch(Actions.updateDates(
      Number(start),
      Number(end)
    ));

    later(() => {
      redux.dispatch(Actions.updateDimension(dimension)).then(() => {
        redux.dispatch(Actions.fetchDimensions(metricId));
      });
    });

    return {
      displayStart,
      displayEnd
    };
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      displayStart,
      displayEnd
    } = model;

    controller.setProperties({
      dimensionsStart: Number(displayStart),
      dimensionsEnd: Number(displayEnd),
      displayStart: Number(displayStart),
      displayEnd: Number(displayEnd)
    });
  },

  actions: {
    // Dispatches a redux action on query param change
    queryParamsDidChange(changedParams, oldParams) {
      const redux = this.get('redux');
      let shouldReload = false;
      const controller = this.controller;
      let {
        analysisStart: start,
        analysisEnd: end,
        displayStart,
        displayEnd
      } = changedParams;
      const params = Object.keys(changedParams || {});

      if (params.length) {
        redux.dispatch(Actions.loading());
        if ((start || end)) {
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

          controller.setProperties({
            displayStart: Number(displayStart),
            dimensionsStart: Number(displayStart)
          });
          shouldReload = true;
        }

        if (controller && displayEnd) {

          controller.setProperties({
            displayEnd: Number(displayEnd),
            dimensionsEnd: Number(displayEnd)
          });
          shouldReload = true;
        }
        if (shouldReload) {
          later(() => {
            redux.dispatch(Actions.loaded());
          });
          shouldReload = false;
        }
      }

      this._super(...arguments);

      return true;
    }
  }
});
