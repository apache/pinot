import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
import { Actions as AnomalyActions } from 'thirdeye-frontend/actions/anomaly';

export default Route.extend({
  redux: service(),
  session: service(),

  model(params) {
    const { id } = params;
    const redux = this.get('redux');

    redux.dispatch(AnomalyActions.fetchData(id));
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
      return true;
    }
  }
});
