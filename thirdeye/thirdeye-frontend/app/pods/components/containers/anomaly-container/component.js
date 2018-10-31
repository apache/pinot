import Component from '@ember/component';
import { connect } from 'ember-redux';
import { Actions } from 'thirdeye-frontend/actions/anomaly';

function select(store) {
  const {
    entity,
    loading,
    loaded,
    failed
  } = store.anomaly;

  return {
    loading,
    loaded,
    failed,
    entity
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

export default connect(select, actions)(Component.extend({
}));
