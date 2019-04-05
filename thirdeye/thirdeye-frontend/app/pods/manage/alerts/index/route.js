import { hash } from 'rsvp';
import Route from '@ember/routing/route';
import fetch from 'fetch';
import { get, getWithDefault } from '@ember/object';
import { inject as service } from '@ember/service';
import { checkStatus, formatYamlFilter } from 'thirdeye-frontend/utils/utils';
import { powerSort } from 'thirdeye-frontend/utils/manage-alert-utils';

// Maps filter name to alert property for filtering
const filterToPropertyMap = {
  application: 'application',
  subscription: 'group',
  owner: 'createdBy',
  type: 'type',
  metric: 'metric'
};

export default Route.extend({

  // Make duration service accessible
  durationCache: service('services/duration'),
  session: service(),

  model() {
    return hash({
      applications: fetch('/thirdeye/entity/APPLICATION').then(checkStatus),
      detectionAlertConfig: fetch('/thirdeye/entity/DETECTION_ALERT_CONFIG').then(checkStatus),
      polishedDetectionYaml: fetch('/yaml/list').then(checkStatus)
    });
  },

  afterModel(model) {
    this._super(model);

    // Fetch all the detection alerts
    const alerts = model.polishedDetectionYaml;
    for (let yamlAlert of alerts) {
      let dimensions = '';
      let dimensionsArray = yamlAlert.dimensionExploration ? yamlAlert.dimensionExploration.dimensions : null;
      if (Array.isArray(dimensionsArray)) {
        dimensionsArray.forEach(dim => {
          dimensions = dimensions + `${dim}, `;
        });
        dimensions = dimensions.substring(0, dimensions.length-2);
      }
      Object.assign(yamlAlert, {
        functionName: yamlAlert.detectionName,
        collection: yamlAlert.dataset,
        type: this._detectionType(yamlAlert),
        exploreDimensions: dimensions,
        filters: formatYamlFilter(yamlAlert.filters),
        isNewPipeline: true
      });
    }

    // Iterate through detection alerter to enhance all yaml alert with extra properties (group name, application)
    for (let subscriptionGroup of model.detectionAlertConfig){
      const detectionConfigIds = Object.keys(subscriptionGroup.vectorClocks);
      for (let id of detectionConfigIds) {
        let foundAlert = alerts.find(yamlAlert => yamlAlert.id.toString() === id);
        if (foundAlert) {
          Object.assign(foundAlert, {
            application: subscriptionGroup.application,
            group: foundAlert.group ? foundAlert.group + ", " + subscriptionGroup.name : subscriptionGroup.name
          });
        }
      }
    }

    // Perform initial filters for our 'primary' filter types and add counts
    const user = getWithDefault(get(this, 'session'), 'data.authenticated.name', null);
    const myAlertIds = user ? this._findAlertIdsByUserGroup(user, model.detectionAlertConfig) : [];
    const ownedAlerts = alerts.filter(alert => alert.createdBy === user);
    const subscribedAlerts = alerts.filter(alert => myAlertIds.includes(alert.id));
    const totalCounts = [subscribedAlerts.length, ownedAlerts.length, alerts.length];
    // Add these filtered arrays to the model (they are only assigne once)
    Object.assign(model, { alerts, ownedAlerts, subscribedAlerts, totalCounts });
  },

  setupController(controller, model) {

    // This filter category is "global" in nature. When selected, they reset the rest of the filters
    const filterBlocksGlobal = [
      {
        name: 'primary',
        type: 'link',
        preventCollapse: true,
        totals: model.totalCounts,
        selected: ['All alerts'],
        filterKeys: ['Alerts I subscribe to', 'Alerts I own', 'All alerts']
      }
    ];

    // This filter category is "secondary". To add more, add an entry here and edit the controller's "filterToPropertyMap"
    const filterBlocksLocal = [
      {
        name: 'status',
        title: 'Status',
        type: 'checkbox',
        selected: ['Active', 'Inactive'],
        filterKeys: ['Active', 'Inactive']
      },
      {
        name: 'application',
        title: 'Applications',
        type: 'select',
        matchWidth: true,
        hasNullOption: true, // allow searches for 'none'
        filterKeys: []
      },
      {
        name: 'subscription',
        title: 'Subscription Groups',
        hasNullOption: true, // allow searches for 'none'
        type: 'select',
        filterKeys: []
      },
      {
        name: 'owner',
        title: 'Owners',
        type: 'select',
        matchWidth: true,
        filterKeys: []
      },
      {
        name: 'type',
        title: 'Detection Type',
        type: 'select',
        filterKeys: []
      },
      {
        name: 'metric',
        title: 'Metrics',
        type: 'select',
        filterKeys: []
      }
    ];

    // Fill in select options for these filters ('filterKeys') based on alert properties from model.alerts
    filterBlocksLocal.filter(block => block.type === 'select').forEach((filter) => {
      const alertPropertyArray = model.alerts.map(alert => alert[filterToPropertyMap[filter.name]]);
      const filterKeys = [ ...new Set(powerSort(alertPropertyArray, null))];
      // Add filterKeys prop to each facet or filter block
      Object.assign(filter, { filterKeys });
    });

    // Keep an initial copy of the secondary filter blocks in memory
    Object.assign(model, {
      initialFiltersGlobal: filterBlocksGlobal,
      initialFiltersLocal: filterBlocksLocal
    });

    // Send filters to controller
    controller.setProperties({
      model,
      resultsActive: true,
      filterToPropertyMap,
      filterBlocksGlobal,
      filterBlocksLocal,
      filteredAlerts: model.alerts,
      totalFilteredAlerts: model.alerts.length,
      sortModes: ['Edited:first', 'Edited:last', 'A to Z', 'Z to A'] // Alerts Search Mode options
    });
  },

  /**
   * Grab detection type if available, else return yamlAlert.pipelineType
   */
  _detectionType(yamlAlert) {
    if (yamlAlert.rules && Array.isArray(yamlAlert.rules) && yamlAlert.rules.length > 0) {
      if (yamlAlert.rules[0].detection && Array.isArray(yamlAlert.rules[0].detection) && yamlAlert.rules[0].detection.length > 0) {
        return yamlAlert.rules[0].detection[0].type;
      }
    }
    return yamlAlert.pipelineType;
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
           if (filter && Array.isArray(filter)) {
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
   },

  /**
   * A local helper to find "Alerts I subscribe to"
   * @method _findAlertIdsByUserGroup
   * @param {String} user - current logged in user's email alias
   * @param {Array} subscriberGroups - all subscription groups in model
   * @return {Array} - array of alert Ids current user subscribes to
   */
  _findAlertIdsByUserGroup(user, subscriberGroups) {
    const isLookupLenient = true; // For 'alerts I subscribe to'
    // Find subscription groups current user is associated with
    const myGroups = subscriberGroups.filter((group) => {
      let userInRecipients = getWithDefault(group, 'properties.recipients.to', []).includes(user);
      let userAnywhere = userInRecipients || group.updatedBy === user || group.createdBy === user;
      return isLookupLenient ? userAnywhere : userInRecipients;
    });
    // Extract alert ids from these groups
    const myAlertIds = [ ...new Set(myGroups
      .map(group => getWithDefault(group, 'properties.detectionConfigIds', []))
      .reduce((a, b) => [...a, ...b], [])
    )];
    return myAlertIds;
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
