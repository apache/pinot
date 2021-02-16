/**
 * Handles the 'alert details' route.
 * @module manage/alert/route
 * @exports manage alert model
 */
import Route from '@ember/routing/route';
import RSVP from 'rsvp';
import { set, get } from '@ember/object';
import { inject as service } from '@ember/service';
import { toastOptions } from 'thirdeye-frontend/utils/constants';
import { formatYamlFilter, redundantParse } from 'thirdeye-frontend/utils/yaml-tools';
import moment from 'moment';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

const ANALYSIS_RANGE = [
  moment().add(1, 'day').subtract(30, 'day').startOf('day').valueOf(),
  moment().add(1, 'day').startOf('day').valueOf()
];

export default Route.extend(AuthenticatedRouteMixin, {
  notifications: service('toast'),
  analysisRange: ANALYSIS_RANGE,

  async model(params) {
    const alertId = params.alert_id;
    const analysisRange = get(this, 'analysisRange');
    const getProps = {
      method: 'get',
      headers: { 'content-type': 'application/json' }
    };
    const notifications = get(this, 'notifications');

    //detection alert fetch
    const detectionUrl = `/detection/${alertId}`;
    try {
      const detection_result = await fetch(detectionUrl, getProps);
      const detection_status = get(detection_result, 'status');
      const detection_json = await detection_result.json();
      if (detection_status !== 200) {
        if (detection_status !== 401) {
          notifications.error('Retrieval of alert yaml failed.', 'Error', toastOptions);
        }
      } else {
        if (detection_json.yaml) {
          let detectionInfo;
          try {
            detectionInfo = redundantParse(detection_json.yaml);
          } catch (error) {
            throw new Error('yaml parsing error');
          }
          const lastDetection = new Date(detection_json.lastTimestamp);
          Object.assign(detectionInfo, {
            isActive: detection_json.active,
            createdBy: detection_json.createdBy,
            updatedBy: detection_json.updatedBy,
            exploreDimensions: detection_json.dimensions,
            dataset: detection_json.datasetNames,
            metric: detection_json.metric,
            filters: formatYamlFilter(detectionInfo.filters),
            dimensionExploration: formatYamlFilter(detectionInfo.dimensionExploration),
            lastDetectionTime:
              lastDetection.toDateString() +
              ', ' +
              lastDetection.toLocaleTimeString() +
              ' (' +
              moment().tz(moment.tz.guess()).format('z') +
              ')',
            rawYaml: detection_json.yaml
          });

          this.setProperties({
            alertId: alertId,
            detectionInfo,
            rawDetectionYaml: detection_json.yaml,
            metricUrn: detection_json.metricUrns[0],
            metricUrnList: detection_json.metricUrns,
            timeWindowSize: detection_json.alertDetailsDefaultWindowSize,
            granularity: detection_json.monitoringGranularity.toString()
          });
        }
      }
    } catch (error) {
      notifications.error('Retrieving alert yaml failed.', error, toastOptions);
    }

    //detection health fetch
    const healthUrl = `/detection/health/${alertId}?start=${analysisRange[0]}&end=${analysisRange[1]}`;
    try {
      const health_result = await fetch(healthUrl, getProps);
      const health_status = get(health_result, 'status');
      const health_json = await health_result.json();
      if (health_status !== 200) {
        if (health_status !== 401) {
          notifications.error('Retrieval of detection health failed.', 'Error', toastOptions);
        }
      } else {
        set(this, 'detectionHealth', health_json);
      }
    } catch (error) {
      notifications.error('Retrieval of detection health failed.', 'Error', toastOptions);
    }

    //subscription group fetch
    const subUrl = `/detection/subscription-groups/${alertId}`; //dropdown of subscription groups
    try {
      const settings_result = await fetch(subUrl, getProps);
      const settings_status = get(settings_result, 'status');
      const settings_json = await settings_result.json();
      if (settings_status !== 200) {
        if (settings_status !== 401) {
          notifications.error('Retrieval of subscription groups failed.', 'Error', toastOptions);
        }
      } else {
        set(this, 'subscriptionGroups', settings_json);
      }
    } catch (error) {
      notifications.error('Retrieving subscription groups failed.', error, toastOptions);
    }

    let subscribedGroups = '';
    if (typeof get(this, 'subscriptionGroups') === 'object' && get(this, 'subscriptionGroups').length > 0) {
      const groups = get(this, 'subscriptionGroups');
      for (let key in groups) {
        if ({}.propertyIsEnumerable.call(groups, key)) {
          let group = groups[key];
          if (subscribedGroups === '') {
            subscribedGroups = group.name;
          } else {
            subscribedGroups = subscribedGroups + ', ' + group.name;
          }
        }
      }
    }

    return RSVP.hash({
      alertId,
      alertData: get(this, 'detectionInfo'),
      detectionYaml: get(this, 'rawDetectionYaml'),
      detectionHealth: get(this, 'detectionHealth'),
      subscribedGroups,
      metricUrn: get(this, 'metricUrn'),
      metricUrnList: get(this, 'metricUrnList') ? get(this, 'metricUrnList') : [],
      timeWindowSize: get(this, 'timeWindowSize'),
      granularity: get(this, 'granularity')
    });
  },

  /**
   * Ember lifecycle hook. Redirect to the subroute depending on the type of the alert
   *
   * @override
   */
  redirect({ alertData: { type } = {} } = {}) {
    if (type === 'COMPOSITE_ALERT') {
      this.transitionTo('manage.explore.composite-anomalies');
    } else {
      this.transitionTo('manage.explore.single-metric-anomalies');
    }
  },

  actions: {
    /**
     * save session url for transition on login
     * @method willTransition
     */
    willTransition(transition) {
      //saving session url - TODO: add a util or service - lohuynh
      if (transition.intent.name && transition.intent.name !== 'logout') {
        this.set('session.store.fromUrl', { lastIntentTransition: transition });
      }
    },

    error() {
      return true;
    },

    /**
     * Refresh route's model.
     * @method refreshModel
     * @return {undefined}
     */
    refreshModel() {
      this.refresh();
    }
  }
});
