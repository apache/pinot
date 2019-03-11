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
    const detectionUrl = `/detection/${alertId}`;
    try {
      const detection_result = await fetch(detectionUrl, postProps);
      const detection_status  = get(detection_result, 'status');
      const detection_json = await detection_result.json();
      if (detection_status !== 200) {
        notifications.error('Retrieval of alert yaml failed.', 'Error');
      } else {
        if (detection_json.yaml) {
          const detectionYaml = yamljs.parse(detection_json.yaml);
          const lastDetection = new Date(detection_json.lastTimestamp);
          Object.assign(detectionYaml, {
            lastDetectionTime: lastDetection.toDateString() + ", " +  lastDetection.toLocaleTimeString() + " (" + Intl.DateTimeFormat().resolvedOptions().timeZone + ")",
            isActive: detection_json.active,
            createdBy: detection_json.createdBy,
            updatedBy: detection_json.updatedBy,
            functionName: detectionYaml.detectionName,
            collection: detectionYaml.dataset,
            type: detectionYaml.pipelineType,
            exploreDimensions: detection_json.dimensions,
            filters: this._formatYamlFilter(detectionYaml.filters),
            dimensionExploration: this._formatYamlFilter(detectionYaml.dimensionExploration),
            yaml: detection_json.yaml
          });

          this.setProperties({
            alertHeaderFields: detectionYaml,
            alertId: alertId,
            metricUrn: detection_json.properties.nested[0].nestedMetricUrns[0],
            metricUrnList: detection_json.properties.nested[0].nestedMetricUrns
          });

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
      alertData: get(this, 'alertHeaderFields'),
      detectionYaml: get(this, 'detectionYaml') ? get(this, 'detectionYaml').yaml : null,
      subscriptionGroups: get(this, 'subscriptionGroups'),
      subscriptionGroupYamlDisplay,
      metricUrn: get(this, 'metricUrn'),
      metricUrnList: get(this, 'metricUrnList') ? get(this, 'metricUrnList') : []
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
          if (filter && typeof filter === 'object') {

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
