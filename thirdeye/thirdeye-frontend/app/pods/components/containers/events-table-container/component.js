import Component from '@ember/component';
import { connect } from 'ember-redux';

function select(store) {
  const {
    loading,
    loaded,
    events,
    eventStart,
    eventEnd
  } = store.events;

  const {
    selectedEvents = []
  } = store.primaryMetric;

  return {
    loading,
    loaded,
    eventStart,
    eventEnd,
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

export default connect(select, actions)(Component.extend({
}));
