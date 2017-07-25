import Ember from 'ember';
import RSVP from 'rsvp';
import moment from 'moment';
import fetch from 'fetch';
import { Actions } from 'thirdeye-frontend/actions/primary-metric';

import { Actions as MetricsActions } from 'thirdeye-frontend/actions/metrics';
import { Actions as DimensionsActions } from 'thirdeye-frontend/actions/dimensions';
import { Actions as EventsActions } from 'thirdeye-frontend/actions/events';


const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Ember.Route.extend({
  // queryParams for rca
  queryParams: {
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    granularity: queryParamsConfig,
    filters: queryParamsConfig,
    compareMode: queryParamsConfig,
    analysisStart: {replace: true},
    analysisEnd: {replace: true}
  },

  redux: Ember.inject.service(),

  // resets all redux stores' state
  beforeModel(transition) {
    const redux = this.get('redux');

    if (transition.targetName === 'rca.details.index') {
      this.replaceWith('rca.details.events');
    }

    return redux.dispatch(Actions.reset())
      .then(() => redux.dispatch(MetricsActions.reset()))
      .then(() => redux.dispatch(DimensionsActions.reset()))
      .then(() => redux.dispatch(EventsActions.reset()));
  },

  model(params) {
    const { metricId: id } = params;
    if (!id) { return; }

    return RSVP.hash({
      granularities: fetch(`/data/agg/granularity/metric/${id}`).then(res => res.json()),
      metricFilters: fetch(`/data/autocomplete/filters/metric/${id}`).then(res => res.json()),
      maxTime: fetch(`/data/maxDataTime/metricId/${id}`).then(res => res.json()),
      id
    });
  },

  afterModel(model, transition) {
    const redux = this.get('redux');
    const maxTime = moment(model.maxTime);
    let start = null;

    const {
      startDate,
      endDate = moment().subtract(1, 'day').endOf('day').valueOf(),
      analysisStart,
      analysisEnd,
      granularity = model.granularities[0],
      filters = JSON.stringify({}),
      compareMode = 'WoW'
    } = transition.queryParams;


    if (granularity === 'DAYS') {
      start = moment().subtract(29, 'days').startOf('day');
    } else {
      start = moment().subtract(24, 'hours').startOf('hour');
    }


    const params = {
      startDate: startDate || start,
      endDate: maxTime.isValid() ? maxTime.valueOf() : endDate,
      granularity: granularity,
      filters,
      id: model.id,
      analysisStart,
      analysisEnd,
      graphStart: analysisStart,
      graphEnd: analysisEnd,
      compareMode
    };

    Object.assign(model, params);

    redux.dispatch(Actions.setPrimaryMetric(params))
      .then((res) => redux.dispatch(Actions.fetchRegions(res)))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricData(res)));

    return {};
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      granularity,
      startDate,
      endDate,
      filters,
      analysisStart,
      analysisEnd,
      compareMode
    } = model;

    let diff = Math.floor((+endDate - startDate) / 4);
    let initStart = analysisStart || (+startDate + diff);
    let initEnd = analysisEnd || (+endDate - diff);

    controller.setProperties({
      model,
      filters,
      analysisStart: initStart,
      analysisEnd: initEnd,
      extentStart: initStart,
      extentEnd: initEnd,
      granularity,
      startDate,
      endDate,
      compareMode
    });
  },

  actions: {
    willTransition(transition) {
      if (transition.targetName === 'rca.index') {
        this.refresh();
      }
    }
  }
});
