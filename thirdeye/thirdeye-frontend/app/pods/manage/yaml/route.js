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
import moment from 'moment';
import { toastOptions } from 'thirdeye-frontend/utils/constants';
import { formatYamlFilter } from 'thirdeye-frontend/utils/utils';

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
        notifications.error('Retrieval of alert yaml failed.', 'Error', toastOptions);
      } else {
        if (detection_json.yaml) {
          const detectionInfo = yamljs.parse(detection_json.yaml);
          const lastDetection = new Date(detection_json.lastTimestamp);
          Object.assign(detectionInfo, {
            isActive: detection_json.active,
            createdBy: detection_json.createdBy,
            updatedBy: detection_json.updatedBy,
            exploreDimensions: detection_json.dimensions,
            filters: formatYamlFilter(detectionInfo.filters),
            dimensionExploration: formatYamlFilter(detectionInfo.dimensionExploration),
            lastDetectionTime: lastDetection.toDateString() + ", " +  lastDetection.toLocaleTimeString() + " (" + moment().tz(moment.tz.guess()).format('z') + ")"
          });

          this.setProperties({
            alertId: alertId,
            detectionInfo,
            rawDetectionYaml: detection_json.yaml
          });
        }
      }
    } catch (error) {
      notifications.error('Retrieving alert yaml failed.', error, toastOptions);
    }

    //subscription group fetch
    const subUrl = `/detection/subscription-groups/${alertId}`;//dropdown of subscription groups
    try {
      const settings_result = await fetch(subUrl, postProps);
      const settings_status  = get(settings_result, 'status');
      const settings_json = await settings_result.json();
      if (settings_status !== 200) {
        notifications.error('Retrieving subscription groups failed.', 'Error', toastOptions);
      } else {
        set(this, 'subscriptionGroups', settings_json);
      }
    } catch (error) {
      notifications.error('Retrieving subscription groups failed.', error, toastOptions);
    }

    let subscribedGroups = "";
    if (typeof get(this, 'subscriptionGroups') === 'object' && get(this, 'subscriptionGroups').length > 0) {
      const groups = get(this, 'subscriptionGroups');
      for (let key in groups) {
        if (groups.hasOwnProperty(key)) {
          let group = groups[key];
          if (subscribedGroups === "") {
            subscribedGroups = group.name;
          } else {
            subscribedGroups = subscribedGroups + ", " + group.name;
          }
        }
      }
    }

    return RSVP.hash({
      alertId,
      alertData: get(this, 'detectionInfo'),
      detectionYaml: get (this, 'rawDetectionYaml'),
      subscriptionGroups: get(this, 'subscriptionGroups'),
      subscribedGroups
    });
  },


});
