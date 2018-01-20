/**
 * Handles the 'edit' route for manage alert
 * @module manage/alert/edit/edit
 * @exports manage/alert/edit/edit
 */
import fetch from 'fetch';
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

    controller.setProperties({
      model,
      granularity: model.alertData.bucketSize + '_' + model.alertData.bucketUnit,
      alertFilters: model.alertData.filters,
      alertProps: model.propsArray,
      alertConfigGroups: model.allConfigGroups,
      alertFunctionName: model.alertData.functionName,
      alertId: model.alertData.id,
      isActive: model.alertData.isActive,
      allApplications: model.allApps,
      selectedConfigGroup: model.originalConfigGroup,
      selectedApplication: model.selectedApplication,
      selectedAppName: model.selectedAppName,
      isLoadError: model.loadError,
      loadErrorMessage: model.loadErrorMsg
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
