/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import { set, get } from '@ember/object';
import { inject as service } from '@ember/service';
import yamljs from 'yamljs';

export default Route.extend({
  notifications: service('toast'),

  async model(params) {
    const alertId = params.alert_id;
    const postProps = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = get(this, 'notifications');

    //detection alert fetch
    const alertUrl = `/detection/${alertId}`;
    try {
      const alert_result = await fetch(alertUrl, postProps);
      const alert_status  = get(alert_result, 'status');
      const alert_json = await alert_result.json();
      if (alert_status !== 200) {
        notifications.error('Retrieval of alert yaml failed.', 'Error');
      } else {
        if (alert_json.yaml) {
          const yaml = yamljs.parse(alert_json.yaml);
          Object.assign(yaml, {
            application: alert_json.name,
            isActive: alert_json.active,
            createdBy: alert_json.createdBy,
            updatedBy: alert_json.updatedBy,
            functionName: yaml.detectionName,
            collection: yaml.dataset,
            type: alert_json.pipelineType,
            exploreDimensions: alert_json.dimensions,
            filters: this._formatYamlFilter(yaml.filters),
            dimensionExploration: this._formatYamlFilter(yaml.dimensionExploration),
            yaml: alert_json.yaml
          });
          set(this, 'detectionYaml', yaml);
        }
      }
    } catch (error) {
      notifications.error('Retrieving alert yaml failed.', error);
    }

    //subscription group fetch
    const subUrl = `/detection/subscription-groups/${alertId}`;//dropdown of subscription groups
    try {
      const settings_result = await fetch(subUrl, postProps);
      const settings_status  = get(settings_result, 'status');
      const settings_json = await settings_result.json();
      if (settings_status !== 200) {
        notifications.error('Retrieving subscription groups failed.', 'Error');
      } else {
        set(this, 'subscriptionGroups', settings_json);
      }
    } catch (error) {
      notifications.error('Retrieving subscription groups failed.', error);
    }

    const subscriptionGroupYamlDisplay = typeof get(this, 'subscriptionGroups') === 'object' && get(this, 'subscriptionGroups').length > 0 ? get(this, 'subscriptionGroups')[0].yaml : get(this, 'subscriptionGroups').yaml;
    const subscriptionGroupId = typeof get(this, 'subscriptionGroups') === 'object' && get(this, 'subscriptionGroups').length > 0 ? get(this, 'subscriptionGroups')[0].id : get(this, 'subscriptionGroups').id;
    return RSVP.hash({
      alertId,
      subscriptionGroupId,
      alertData: get(this, 'detectionYaml'),
      detectionYaml: get(this, 'detectionYaml').yaml,
      subscriptionGroups: get(this, 'subscriptionGroups'),
      subscriptionGroupYamlDisplay
    });
  },

  /**
   * The yaml filters formatter. Convert filters in the yaml file in to a legacy filters string
   * For example, filters = {
   *   "country": ["us", "cn"],
   *   "browser": ["chrome"]
   * }
   * will be convert into "country=us;country=cn;browser=chrome"
   *
   * @method _formatYamlFilter
   * @param {Map} filters multimap of filters
   * @return {String} - formatted filters string
   */
  _formatYamlFilter(filters) {
    if (filters){
      const filterStrings = [];
      Object.keys(filters).forEach(
        function(filterKey) {
          const filter = filters[filterKey];
          if (typeof filter === 'object') {
            filter.forEach(
              function (filterValue) {
                filterStrings.push(filterKey + '=' + filterValue);
              }
            );
          } else {
            filterStrings.push(filterKey + '=' + filter);
          }
        }
      );
      return filterStrings.join(';');
    }
    return '';
  }
});
