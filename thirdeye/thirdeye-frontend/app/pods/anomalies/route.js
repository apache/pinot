import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import moment from 'moment';
import { inject as service } from '@ember/service';
import { powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';
import {  getAnomalyIdsByTimeRange } from 'thirdeye-frontend/utils/anomaly';

const start = moment().subtract(1, 'day').valueOf();
const end = moment().valueOf();

export default Route.extend({

  // Make duration service accessible
  durationCache: service('services/duration'),
  session: service(),

  model() {
    return hash({
      updateAnomalies:  getAnomalyIdsByTimeRange,
      anomaliesById: getAnomalyIdsByTimeRange(start, end)
    });
  },

  setupController(controller, model) {

    // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
    const filterBlocksLocal = [
      {
        name: 'statusFilterMap',
        title: 'Anomaly Status',
        type: 'select',
        matchWidth: true,
        filterKeys: []
      },
      {
        name: 'functionFilterMap',
        title: 'Functions',
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
      }
    ];

    // Fill in select options for these filters ('filterKeys') based on alert properties from model.alerts
    filterBlocksLocal.forEach((filter) => {
      if (filter.name === "dimensionFilterMap") {
        const anomalyPropertyArray = Object.keys(model.anomaliesById.searchFilters[filter.name]);
        let filterKeys = [];
        anomalyPropertyArray.forEach(dimensionType => {
          let group = Object.keys(model.anomaliesById.searchFilters[filter.name][dimensionType]);
          group = group.map(dim => `${dimensionType}::${dim}`);
          filterKeys = [...filterKeys, ...group];
        });
        Object.assign(filter, { filterKeys });
      } else {
        const anomalyPropertyArray = Object.keys(model.anomaliesById.searchFilters[filter.name]);
        const filterKeys = [ ...new Set(powerSort(anomalyPropertyArray, null))];
        // Add filterKeys prop to each facet or filter block
        Object.assign(filter, { filterKeys });
      }
    });

    // Keep an initial copy of the secondary filter blocks in memory
    Object.assign(model, {
      initialFiltersLocal: filterBlocksLocal
    });

    // Send filters to controller
    controller.setProperties({
      model,
      anomaliesById: model.anomaliesById,
      resultsActive: true,
      updateAnomalies: model.updateAnomalies,  //requires start and end time in epoch ex updateAnomalies(start, end)
      filterBlocksLocal,
      anomalyIds: model.anomaliesById.anomalyIds,
      anomaliesRange: [start, end]
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
