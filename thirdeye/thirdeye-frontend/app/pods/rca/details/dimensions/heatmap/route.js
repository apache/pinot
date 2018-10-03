import { later } from '@ember/runloop';
import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import { Actions } from 'thirdeye-frontend/actions/dimensions';

export default Route.extend({
  redux: service(),
  session: service(),
  model(params, transition) {
    const redux = this.get('redux');
    const { metric_id } = transition.params['rca.details'];
    const {
      analysisStart: initStart,
      analysisEnd: initEnd
    } = this.modelFor('rca.details');
    const {
      analysisStart,
      analysisEnd
    } = transition.queryParams;

    if (!metric_id) { return; }
    const start = analysisStart || initStart;
    const end = analysisEnd || initEnd;

    redux.dispatch(Actions.fetchHeatMapData(Number(start), Number(end)));
    return {};
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', {lastIntentTransition: transition});
      }
    },
    error() {
      // The `error` hook is also provided the failed
      // `transition`, which can be stored and later
      // `.retry()`d if desired.
      return true;
    },

    // Dispatches a redux action on query param change
    // to fetch heatmap data in the new date range
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

        later(() => {
          redux.dispatch(Actions.fetchHeatMapData(
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
