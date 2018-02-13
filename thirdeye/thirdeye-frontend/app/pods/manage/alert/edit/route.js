/**
 * Handles the 'edit' route for manage alert
 * @module manage/alert/edit/edit
 * @exports manage/alert/edit/edit
 */
import _ from 'lodash';
import Route from '@ember/routing/route';

export default Route.extend({
  model(params) {
    const { id, alertData, email, allConfigGroups, allAppNames } = this.modelFor('manage.alert');
    if (!id) { return; }

    return {
       alertData,
       email,
       allConfigGroups,
       allAppNames
    };
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

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
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
  },

  actions: {
    /**
     * Action called on submission to reload the route's model
     */
    refreshModel() {
      this.refresh();
    }
  }
});
