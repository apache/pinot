/**
 * Handles the 'edit' route for manage alert
 * @module manage/alert/edit/edit
 * @exports manage/alert/edit/edit
 */
import _ from 'lodash';
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
      alertData,
      allConfigGroups,
      allAppNames
    } = this.modelFor('manage.alert');

    if (!id) { return; }
    const email = await fetch(selfServeApiCommon.configGroupByAlertId(id)).then(checkStatus);

    return RSVP.hash({
      email,
      alertData,
      allConfigGroups,
      allAppNames
    });
  },

  afterModel(model) {
    const {
      alertData,
      email: groupByAlertId,
      allConfigGroups,
      allAppNames
    } = model;

    const {
      id,
      filters,
      bucketSize,
      bucketUnit,
      properties: alertProps
    } = alertData;

    // Add a parsed properties array to the model
    const propsArray = alertProps.split(';').map((prop) => {
      const [ name, value ] = prop.split('=');
      return { name, value: decodeURIComponent(value) };
    });

    const originalConfigGroup = groupByAlertId.length ? groupByAlertId.pop() : null;
    const selectedAppName = originalConfigGroup ? originalConfigGroup.application : null;
    const selectedApplication = _.find(allAppNames, function(appsObj) { return appsObj.application === selectedAppName; });

    Object.assign(model, {
      propsArray,
      allConfigGroups: _.uniq(allConfigGroups, name),
      originalConfigGroup,
      selectedAppName,
      allApps: allAppNames,
      selectedApplication
    });
  },

  setupController(controller, model) {
    this._super(controller, model);

    const {
      alertData,
      selectedAppName,
      selectedApplication,
      allApps: allApplications,
      propsArray: alertProps,
      loadError: isLoadError,
      loadErrorMsg: loadErrorMessage,
      allConfigGroups: alertConfigGroups,
      originalConfigGroup: selectedConfigGroup
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
      alertConfigGroups,
      alertFunctionName,
      alertId,
      isActive,
      allApplications,
      selectedConfigGroup,
      selectedApplication,
      selectedAppName,
      isLoadError,
      loadErrorMessage,
      granularity: `${bucketSize}_${bucketUnit}`
    });

    // Populate 'alerts monitored' table with currently selected config group
    get(this, 'prepareAlertList').perform(selectedConfigGroup);
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

  /**
   * Fetch alert data for each function id that the currently selected group watches
   * @method prepareAlertList
   * @param {Object} configGroup - currently selected config group
   * @return {undefined}
   */
  prepareAlertList: task(function * (configGroup) {
    const functionIds = getWithDefault(configGroup, 'emailConfig.functionIds', []);
    const functionData = yield get(this, 'fetchAlertDataById').perform(functionIds);
    const formattedData = _.values(functionData).map(data => formatConfigGroupProps(data));
    this.controller.set('selectedGroupFunctions', formattedData);
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
