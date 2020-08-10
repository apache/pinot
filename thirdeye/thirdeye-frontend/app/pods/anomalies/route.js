import {hash} from 'rsvp';
import Route from '@ember/routing/route';
import moment from 'moment';
import {inject as service} from '@ember/service';
import {anomalyResponseObj, searchAnomaly} from 'thirdeye-frontend/utils/anomaly';
import _ from 'lodash';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const start = moment().subtract(1, 'day').valueOf();
const end = moment().valueOf();
const pagesize = 10;

const queryParamsConfig = {
  refreshModel: true, replace: false
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
    let {anomalyIds} = params;
    let searchResult;
    if (anomalyIds) {
      anomalyIds = anomalyIds.split(",");
    }
    // query anomalies
    searchResult = searchAnomaly(0, pagesize, anomalyIds ? null : start, anomalyIds ? null : end, anomalyIds);
    return hash({
      updateAnomalies: searchAnomaly, anomalyIds, searchResult
    });
  },

  afterModel(model) {
    // If we set anomalyIds to null in the controller, the route will refresh and clear the params from url
    const anomalyIds = model.anomalyIds || null;
    this.set('anomalyIds', anomalyIds);
    const defaultParams = {
      anomalyIds
    };
    Object.assign(model, {...defaultParams});
    return model;
  },

  setupController(controller, model) {
    // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
    const filterBlocksLocal = [{
      name: 'alertName', title: 'Alert Names', type: 'search', filterKeys: []
    }, {
      name: 'dataset', title: 'Datasets', type: 'search', filterKeys: []
    }, {
      name: 'metric', title: 'Metrics', type: 'search', filterKeys: []
    }, {
      name: 'feedbackStatus',
      title: 'Feedback Status',
      type: 'select',
      matchWidth: true,
      filterKeys: anomalyResponseObj.map(f => f.name)
    }, {
      name: 'subscription', title: 'Subscription Groups', hasNullOption: true, // allow searches for 'none'
      type: 'search', filterKeys: []
    }];

    // Keep an initial copy of the secondary filter blocks in memory
    Object.assign(model, {
      initialFiltersLocal: _.cloneDeep(filterBlocksLocal)
    });
    // Send filters to controller
    controller.setProperties({
      model, resultsActive: true, updateAnomalies: model.updateAnomalies,  //requires start and end time in epoch ex updateAnomalies(start, end)
      filterBlocksLocal, anomaliesRange: [start, end], anomalyIds: this.get('anomalyIds'), // url params
      searchResult: model.searchResult
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
    }, error() {
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
