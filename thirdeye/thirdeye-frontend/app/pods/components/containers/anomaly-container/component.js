import Ember from 'ember';
import connect from 'ember-redux/components/connect';
import { Actions } from 'thirdeye-frontend/actions/anomaly';

function select(store) {
  const { ids, entities, loading, loaded, failed } = store.anomaly;

  return {
    loading,
    loaded,
    failed,
    anomalies: ids.map(id => entities[id])
  };
}

function actions(dispatch) {
  return {
    onLoad() {
      dispatch(Actions.loadAnomaly());
    },
    onLoading() {
      dispatch(Actions.loading());
    },
    onRequest() {
      const params = {};

      dispatch(Actions.request(params));
    }
  };
}

export default connect(select, actions)(Ember.Component.extend({
}));
