import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import moment from 'moment';
import { inject as service } from '@ember/service';
import { isPresent } from '@ember/utils';
import { powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';
import {
  getAnomalyFiltersByTimeRange,
  getAnomalyFiltersByAnomalyId,
  anomalyResponseObjNew } from 'thirdeye-frontend/utils/anomaly';
import _ from 'lodash';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const start = moment().subtract(1, 'day').valueOf();
const end = moment().valueOf();

const queryParamsConfig = {
  refreshModel: true,
  replace: false
};

export default Route.extend(AuthenticatedRouteMixin, {

  // Make duration service accessible
  durationCache: service('services/duration'),
  anomaliesApiService: service('services/api/anomalies'),
  session: service(),
  store: service('store'),
  queryParams: {
    anomalyIds: queryParamsConfig
  },
  anomalyIds: null,

  async model(params) {
    // anomalyIds param allows for clicking into the route from email and listing a specific set of anomalyIds
    let { anomalyIds } = params;
    const anomaliesById = anomalyIds ? await getAnomalyFiltersByAnomalyId(start, end, anomalyIds) : await getAnomalyFiltersByTimeRange(start, end);
    const subscriptionGroups = await this.get('anomaliesApiService').querySubscriptionGroups(); // Get all subscription groups available
    return hash({
      updateAnomalies:  getAnomalyFiltersByTimeRange,
      anomaliesById,
      subscriptionGroups,
      anomalyIds
    });
  },

  afterModel(model) {
    // If we set anomalyIds to null in the controller, the route will refresh and clear the params from url
    const anomalyIds = model.anomalyIds || null;
    this.set('anomalyIds', anomalyIds);
    const defaultParams = {
      anomalyIds
    };
    Object.assign(model, { ...defaultParams});
    return model;
  },

  setupController(controller, model) {

    // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
    const filterBlocksLocal = [
      {
        name: 'statusFilterMap',
        title: 'Feedback Status',
        type: 'select',
        matchWidth: true,
        filterKeys: []
      },
      {
        name: 'functionFilterMap',
        title: 'Alert Names',
        type: 'select',
        filterKeys: []
      },
      {
        name: 'datasetFilterMap',
        title: 'Dataset',
        type: 'select',
        filterKeys: []
      },
      {
        name: 'metricFilterMap',
        title: 'Metric',
        type: 'select',
        filterKeys: []
      },
      {
        name: 'dimensionFilterMap',
        title: 'Dimension',
        type: 'select',
        matchWidth: true,
        filterKeys: []
      },
      {
        name: 'subscriptionFilterMap',
        title: 'Subscription Groups',
        type: 'select',
        filterKeys: []
      }
    ];

    // Fill in select options for these filters ('filterKeys') based on alert properties from model.alerts
    filterBlocksLocal.forEach((filter) => {
      let filterKeys = [];
      if (filter.name === "dimensionFilterMap" && isPresent(model.anomaliesById.searchFilters[filter.name])) {
        const anomalyPropertyArray = Object.keys(model.anomaliesById.searchFilters[filter.name]);
        anomalyPropertyArray.forEach(dimensionType => {
          let group = Object.keys(model.anomaliesById.searchFilters[filter.name][dimensionType]);
          group = group.map(dim => `${dimensionType}::${dim}`);
          filterKeys = [...filterKeys, ...group];
        });
      } else if (filter.name === "statusFilterMap" && isPresent(model.anomaliesById.searchFilters[filter.name])){
        let anomalyPropertyArray = Object.keys(model.anomaliesById.searchFilters[filter.name]);
        anomalyPropertyArray = anomalyPropertyArray.map(prop => {
          // get the right object
          const mapping = anomalyResponseObjNew.filter(e => (e.status === prop));
          // map the status to name
          return mapping.length > 0 ? mapping[0].name : prop;
        });
        filterKeys = [ ...new Set(powerSort(anomalyPropertyArray, null))];
      } else if (filter.name === "subscriptionFilterMap"){
        filterKeys = this.get('store')
          .peekAll('subscription-groups')
          .sortBy('name')
          .filter(group => (group.get('active') && group.get('yaml')))
          .map(group => group.get('name'));
      } else {
        if (isPresent(model.anomaliesById.searchFilters[filter.name])) {
          const anomalyPropertyArray = Object.keys(model.anomaliesById.searchFilters[filter.name]);
          filterKeys = [ ...new Set(powerSort(anomalyPropertyArray, null))];
        }
      }
      Object.assign(filter, { filterKeys });
    });

    // Keep an initial copy of the secondary filter blocks in memory
    Object.assign(model, {
      initialFiltersLocal: _.cloneDeep(filterBlocksLocal)
    });
    // Send filters to controller
    controller.setProperties({
      model,
      anomaliesById: model.anomaliesById,
      resultsActive: true,
      updateAnomalies: model.updateAnomalies,  //requires start and end time in epoch ex updateAnomalies(start, end)
      filterBlocksLocal,
      anomalyIdList: model.anomaliesById.anomalyIds,
      anomaliesRange: [start, end],
      subscriptionGroups: model.subscriptionGroups,
      anomalyIds: this.get('anomalyIds')
    });
  },

  actions: {
    /**
     * Clear duration cache (time range is reset to default when entering new alert page from index)
     * @method willTransition
     */
    willTransition(transition) {
      this.get('durationCache').resetDuration();
      this.controller.set('isLoading', true);

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

    /**
     * Once transition is complete, remove loader
     */
    didTransition() {
      this.controller.set('isLoading', false);
    },

    /**
    * Refresh route's model.
    * @method refreshModel
    */
    refreshModel() {
      this.refresh();
    }
  }
});
