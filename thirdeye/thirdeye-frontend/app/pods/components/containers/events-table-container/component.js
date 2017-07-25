import Ember from 'ember';
import { connect } from 'ember-redux';

function select(store) {
  const {
    loading,
    loaded,
    events
  } = store.events;

  const {
    selectedEvents = []
  } = store.primaryMetric;

  return {
    loading,
    loaded,
    events: events.map((event) => {
      event = Object.assign({}, event);
      if (selectedEvents.includes(event.urn)) {
        event.isSelected = true;
      }
      return event;
    })
  };
}

function actions() {
  return {};
}

export default connect(select, actions)(Ember.Component.extend({
}));
