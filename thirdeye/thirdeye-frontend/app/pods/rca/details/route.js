import { inject as service } from '@ember/service';
import Route from '@ember/routing/route';
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

const replaceConfig = {
  replace: true
};

export default Route.extend({
  // queryParams for rca
  queryParams: {
    startDate: queryParamsConfig,
    endDate: queryParamsConfig,
    granularity: queryParamsConfig,
    filters: queryParamsConfig,
    compareMode: queryParamsConfig,
    analysisStart: replaceConfig,
    analysisEnd: replaceConfig,
    displayStart: replaceConfig,
    displayEnd: replaceConfig
  },

  redux: service(),

  // resets all redux stores' state
  beforeModel(transition) {
    const redux = this.get('redux');

    if (transition.targetName === 'rca.details.index') {
      this.replaceWith('rca.details.dimensions');
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
      endDate = moment().valueOf(),
      analysisStart,
      analysisEnd,
      displayStart,
      displayEnd,
      granularity = model.granularities[0],
      filters = JSON.stringify({}),
      compareMode = 'WoW'
    } = transition.queryParams;

    const newEndDate = maxTime.isValid() && maxTime.isBefore(moment(+endDate))
      ? maxTime
      : moment(+endDate);

    if (granularity === 'DAYS') {
      start = newEndDate.clone().subtract(29, 'days').startOf('day');
    } else if (granularity === 'HOURS') {
      start = newEndDate.clone().subtract(1, 'week').startOf('day');
    } else {
      start = newEndDate.clone().subtract(24, 'hours').startOf('hour');
    }
    const momentEndDate = moment(+newEndDate);
    let initStart = 0;
    let initEnd = 0;
    let subchartStart = 0;
    let subchartEnd = momentEndDate.clone();

    if (granularity === 'DAYS') {
      initEnd = momentEndDate.clone().startOf('day');
      initStart = initEnd.clone().subtract('1', 'day');
      subchartStart = subchartEnd.clone().subtract(1, 'week').startOf('day');
    } else if (granularity === 'HOURS') {
      initEnd = momentEndDate.clone().startOf('hour');
      initStart = initEnd.clone().subtract('1', 'hour');
      subchartStart = subchartEnd.clone().subtract(1, 'day').startOf('day');
    } else {
      initEnd =  momentEndDate.clone().startOf('hour');
      initStart = initEnd.clone().subtract('1', 'hour');
      subchartStart = subchartEnd.clone().subtract(3, 'hours').startOf('hour');
    }

    const params = {
      startDate: startDate || start.valueOf(),
      endDate: newEndDate.valueOf(),
      granularity: granularity,
      filters,
      id: model.id,
      analysisStart: analysisStart || initStart.valueOf(),
      analysisEnd: analysisEnd || initEnd.valueOf(),
      compareMode,
      subchartStart: displayStart || subchartStart.valueOf(),
      subchartEnd: displayEnd || subchartEnd.valueOf(),
      displayStart: displayStart || subchartStart.valueOf(),
      displayEnd: displayEnd || subchartEnd.valueOf()
    };

    Object.assign(model, params);

    redux.dispatch(Actions.setPrimaryMetric(params))
      .then((res) => redux.dispatch(Actions.fetchRegions(res)))
      .then((res) => redux.dispatch(Actions.fetchRelatedMetricData(res)));

    return {};
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.setProperties({
        model: null,
        filters: {},
        analysisStart: undefined,
        analysisEnd: undefined,
        granularity: undefined,
        startDate: undefined,
        endDate: undefined,
        subchartStart: undefined,
        subchartEnd: undefined,
        displayStart: undefined,
        displayEnd: undefined,
        compareMode: 'WoW'
      });
    }
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
      compareMode,
      subchartEnd,
      subchartStart,
      displayStart,
      displayEnd
    } = model;

    controller.setProperties({
      model,
      filters,
      analysisStart,
      analysisEnd,
      granularity,
      startDate,
      endDate,
      compareMode,
      subchartEnd,
      subchartStart,
      displayStart,
      displayEnd
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
