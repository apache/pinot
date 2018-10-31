import Component from '@ember/component';
import { connect } from 'ember-redux';
import { Actions } from 'thirdeye-frontend/actions/primary-metric';
import _ from 'lodash';
import { task, timeout } from 'ember-concurrency';

function select(store) {
  const {
    loading,
    loaded,
    failed,
    relatedMetricEntities: metricData = {},
    primaryMetricId,
    compareMode,
    granularity,
    currentStart,
    currentEnd,
    graphStart,
    graphEnd,
    selectedDimensions,
    selectedEvents,
    selectedMetricIds,
    color,
    isSelected
  } = store.primaryMetric;

  const {
    relatedMetricEntities,
    regions
  } = store.metrics;

  const {
    dimensions
  } = store.dimensions;

  const {
    events = []
  } = store.events;

  const uiMainMetric = _.merge({}, metricData, regions);
  const uiRelatedMetric = _.merge({}, relatedMetricEntities, regions);

  return {
    loading,
    loaded,
    failed,
    compareMode,
    granularity,
    currentStart,
    currentEnd,
    graphStart,
    graphEnd,
    selectedDimensions: selectedDimensions
      .map((key) => {

        const dimension = dimensions[key];
        if (!dimension) { return; }

        return Object.assign({},
          dimension,
          { isSelected });
      }).filter(dimension => dimension),
    primaryMetric: Object.assign({}, uiMainMetric[primaryMetricId], {color}, {isSelected}),
    selectedMetrics: selectedMetricIds
      .map((id) => {

        const metric = uiRelatedMetric[id];
        if (!metric) { return; }

        return Object.assign({},
          metric,
          { isSelected });
      }).filter(metric => metric && metric.metricName),
    selectedEvents: events
      .filter((event) => {
        return selectedEvents.includes(event.urn);
      }).map((event) => Object.assign({}, event, { isSelected }))
  };
}

function actions(dispatch) {
  return {
    /**
     * Ember concurrency task that dispatches new analysis dates
     */
    dateChangeTask: task(function* ([start, end]) {
      yield timeout(1000);

      dispatch(Actions.updateAnalysisDates(start, end));
      return [start, end];
    }).restartable(),

    /**
     * Handles date changes in subchart
     */
    onDateChange(dates) {
      const task = this.get('actions.dateChangeTask');

      return task.perform(dates);
    },

    /**
     * Handles primary selection
     */
    onPrimaryClick() {
      dispatch(Actions.selectPrimary());
    },

    /**
     * Handles dimension selection
     */
    onSelection(name) {
      dispatch(Actions.selectDimension(name));
    },

    /**
     * Handles event selection
     */
    onEventSelection(name) {
      dispatch(Actions.selectEvent(name));
    },

    /**
     * Handles metric selection
     */
    onMetricSelection(metric) {
      dispatch(Actions.selectMetric(metric));
    },


    /**
     * Handles deselection of an entity
     */
    onDeselect(entity) {
      if (this.get('selectedDimensions').includes(entity)) {
        dispatch(Actions.selectDimension(entity));
      }

      if (_.map(this.get('selectedEvents'), 'urn').includes(entity)) {
        dispatch(Actions.selectEvent(entity));
      }
      if (this.get('selectedMetrics').includes(entity)) {
        dispatch(Actions.selectMetric(entity));
      }
    }
  };
}

export default connect(select, actions)(Component.extend({
}));
