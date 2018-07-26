/**
 * Handles the 'edit' route for manage alert
 * @module manage/alert/edit/edit
 * @exports manage/alert/edit/edit
 */
import RSVP from 'rsvp';
import fetch from 'fetch';
import Route from '@ember/routing/route';
import { task, timeout } from 'ember-concurrency';
import { get, getWithDefault } from '@ember/object';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { selfServeApiCommon } from 'thirdeye-frontend/utils/api/self-serve';
import { formatConfigGroupProps } from 'thirdeye-frontend/utils/manage-alert-utils';

export default Route.extend({

/**
 * Optional params to load a fresh view
 */
  queryParams: {
    refresh: {
      refreshModel: true,
      replace: false
    }
  },

  async model(params, transition) {
    const {
      id,
      alertData
    } = this.modelFor('manage.alert');

    if (!id) { return; }

    const alertGroups = await fetch(selfServeApiCommon.configGroupByAlertId(id)).then(checkStatus);

    return RSVP.hash({
      alertGroups,
      alertData
    });
  },

  afterModel(model) {
    const {
      alertData,
      alertGroups
    } = model;

    const {
      properties: alertProps
    } = alertData;

    // Add a parsed properties array to the model
    const propsArray = alertProps.split(';').map((prop) => {
      const [ name, value ] = prop.split('=');
      return { name, value: decodeURIComponent(value) };
    });

    Object.assign(model, {
      propsArray,
      alertGroups
    });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      alertData,
      alertGroups,
      propsArray: alertProps,
      loadError: isLoadError,
      loadErrorMsg: loadErrorMessage
    } = model;

    const {
      isActive,
      bucketSize,
      bucketUnit,
      id: alertId,
      filters: alertFilters,
      functionName: alertFunctionName
    } = alertData;

    controller.setProperties({
      model,
      alertData,
      alertFilters,
      alertProps,
      alertFunctionName,
      alertId,
      alertGroups,
      isActive,
      isLoadError,
      loadErrorMessage,
      granularity: `${bucketSize}_${bucketUnit}`
    });
  },

  /**
   * Fetch alert data for each function id that the currently selected group watches
   * @method fetchAlertDataById
   * @param {Object} functionIds - alert ids included in the currently selected group
   * @return {RSVP.hash} A new list of functions (alerts)
   */
  fetchAlertDataById: task(function * (functionIds) {
    const functionArray = yield functionIds.map(id => fetch(selfServeApiCommon.alertById(id)).then(checkStatus));
    return RSVP.hash(functionArray);
  }),

  actions: {
    /**
     * Action called on submission to reload the route's model
     */
    refreshModel() {
      this.refresh();
    },

    /**
    * Refresh anomaly data when changes are made
    */
    loadFunctionsTable(selectedConfigGroup) {
      get(this, 'prepareAlertList').perform(selectedConfigGroup);
    }
  }
});
