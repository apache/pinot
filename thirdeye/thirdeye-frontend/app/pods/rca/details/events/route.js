import { later } from '@ember/runloop';
import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import { Actions } from 'thirdeye-frontend/actions/events';

export default Route.extend({
  redux: service(),

  /**
   * Massages Query Params from URL and dispatch redux actions
   */
  model(params, transition) {
    const redux = this.get('redux');
    const { metricId } = transition.params['rca.details'];
    const {
      displayStart,
      displayEnd,
      analysisStart: start,
      analysisEnd: end
    } = transition.queryParams;

    if (!metricId) { return; }

    redux.dispatch(Actions.fetchEvents(Number(start), Number(end)));

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
      eventsStart: Number(displayStart),
      eventsEnd: Number(displayEnd),
      displayStart: Number(displayStart),
      displayEnd: Number(displayEnd)
    });
  },

  actions: {
    // Dispatches a redux action on query param change
    // to fetch events in the new date range
    queryParamsDidChange(changedParams, oldParams) {
      const redux = this.get('redux');
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
          controller.setProperties({
            displayStart: Number(displayStart),
            eventsStart: Number(displayStart)
          });
        }

        if (controller && displayEnd) {
          controller.setProperties({
            displayEnd: Number(displayEnd),
            eventsEnd: Number(displayEnd)
          });
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
