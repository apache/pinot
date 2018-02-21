/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import { reads } from '@ember/object/computed';

import RSVP from "rsvp";
import _ from 'lodash';
import fetch from 'fetch';
import Controller from '@ember/controller';
import { computed, set } from '@ember/object';
import { task, timeout } from 'ember-concurrency';
import {
  isPresent,
  isEmpty,
  isNone,
  isBlank
} from "@ember/utils";
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import {
  setMetricData,
  buildMetricDataUrl,
  getTopDimensions
} from 'thirdeye-frontend/utils/manage-alert-utils';

export default Controller.extend({

  /**
   * Initialized alert creation page settings
   */
  isValidated: false,
  isMetricSelected: false,
  isFormDisabled: false,
  isDimensionError: false,
  isMetricDataInvalid: false,
  isSelectMetricError: false,
  isMetricDataLoading: false,
  isGroupNameDuplicate: false,
  isAlertNameDuplicate: false,
  isFetchingDimensions: false,
  isDimensionFetchDone: false,
  isProcessingForm: false,
  isEmailError: false,
  isDuplicateEmail: false,
  isAlertNameUserModified: false,
  showGraphLegend: false,
  redirectToAlertPage: true,
  metricGranularityOptions: [],
  topDimensions: [],
  originalDimensions: [],
  bsAlertBannerType: 'success',
  graphEmailLinkProps: '',
  dimensionCount: 7,
  availableDimensions: 0,
  selectedSeverityOption: 'Percentage of Change',
  legendText: {
    dotted: {
      text: 'WoW'
    },
    solid: {
      text: 'Observed'
    }
  },

  /**
   * Component property initial settings
   */
  filters: {},
  graphConfig: {},
  selectedFilters: JSON.stringify({}),
  selectedWeeklyEffect: true,

  /**
   * Object to cover basic ield 'presence' validation
   */
  requiredFields: [
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName'
  ],

  /**
   * Array to define alerts table columns for selected config group
   */
  alertsTableColumns: [
    {
      propertyName: 'id',
      title: 'Id',
      className: 'te-form__table-index'
    },
    {
      propertyName: 'name',
      title: 'Alert Name'
    },
    {
      propertyName: 'metric',
      title: 'Alert Metric'
    },
    {
      propertyName: 'type',
      title: 'Alert Type'
    }
  ],

  /**
   * Options for patterns of interest field. These may eventually load from the backend.
   */
  patternsOfInterest: ['Up and Down', 'Up only', 'Down only'],

  /**
   * Mapping user readable pattern and sensitivity to DB values
   */
  optionMap: {
    pattern: {
      'Up and Down': 'UP,DOWN',
      'Up only': 'UP',
      'Down only': 'DOWN'
    },
    sensitivity: {
      'Robust (Low)': 'LOW',
      'Medium': 'MEDIUM',
      'Sensitive (High)': 'HIGH'
    },
    severity: {
      'Percentage of Change': 'weight',
      'Absolute Value of Change': 'deviation'
    }
  },

  /**
   * Severity display options (power-select) and values
   * @type {Object}
   */
  tuneSeverityOptions: computed(
    'optionMap.severity',
    function() {
      const severityOptions = Object.keys(this.get('optionMap.severity'));
      return severityOptions;
    }
  ),

  /**
   * Conditionally display '%' based on selected severity option
   * @type {String}
   */
  sensitivityUnits: computed('selectedSeverityOption', function() {
    const chosenSeverity = this.get('selectedSeverityOption');
    const isNotPercent = chosenSeverity && chosenSeverity.includes('Absolute');
    return isNotPercent ? '' : '%';
  }),

  /**
   * Builds the new autotune filter from custom tuning options
   * @type {String}
   */
  alertFilterObj: computed(
    'selectedSeverityOption',
    'customPercentChange',
    'customMttdChange',
    'selectedPattern',
    function() {
      const {
        severity: severityMap,
        pattern: patternMap
      } = this.getProperties('optionMap').optionMap;

      const {
        selectedPattern,
        customMttdChange,
        customPercentChange,
        selectedSeverityOption: selectedSeverity
      } = this.getProperties('selectedPattern', 'customMttdChange', 'customPercentChange', 'selectedSeverityOption');

      const requiredProps = ['customMttdChange', 'customPercentChange', 'selectedSeverityOption'];
      const isCustomFilterPossible = requiredProps.every(val => isPresent(this.get(val)));
      const filterObj = {
        pattern: patternMap[selectedPattern],
        isCustom: isCustomFilterPossible
      };

      // TODO: move this shared logic into utils
      if (isCustomFilterPossible) {
        const mttdVal = Number.isNaN(Number(customMttdChange)) ? 0 : Number(customMttdChange).toFixed(2);
        const severityThresholdVal = Number.isNaN(Number(customPercentChange)) ? 0 : Number(customPercentChange).toFixed(2);
        const finalSeverity = severityMap[selectedSeverity] === 'deviation' ? severityThresholdVal : (severityThresholdVal/100).toFixed(2);
        Object.assign(filterObj, {
          features: `window_size_in_hour,${severityMap[selectedSeverity]}`,
          mttd: `window_size_in_hour=${mttdVal};${severityMap[selectedSeverity]}=${finalSeverity}`
        });
      }

      return filterObj;
    }
  ),

  /**
   * All selected dimensions to be loaded into graph
   * @returns {Array}
   */
  selectedDimensions: computed(
    'topDimensions',
    'topDimensions.@each.isSelected',
    function() {
      return this.get('topDimensions').filterBy('isSelected');
    }
  ),

  /**
   * Application name field options loaded from our model.
   */
  allApplicationNames: reads('model.allAppNames'),

  /**
   * The list of all existing alert configuration groups.
   */
  allAlertsConfigGroups: reads('model.allConfigGroups'),

  /**
   * Handler for search by function name - using ember concurrency (task)
   * @method searchMetricsList
   * @param {metric} String - portion of metric name used in typeahead
   * @return {Promise}
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url).then(checkStatus);
  }),

  /**
   * Determines if a metric should be filtered out
   * @method isMetricGraphable
   * @param {Object} metric
   * @returns {Boolean}
   */
  isMetricGraphable(metric) {
    return metric
    && metric.subDimensionContributionMap['All'].currentValues
    && metric.subDimensionContributionMap['All'].currentValues.reduce((total, val) => {
      return total + val;
    }, 0);
  },

  /**
   * Fetches an alert function record by Id.
   * Use case: show me the names of all functions monitored by a given alert group.
   * @method fetchFunctionById
   * @param {Number} functionId - Id for the selected alert function
   * @return {Promise}
   */
  fetchFunctionById(functionId) {
    const url = `/onboard/function/${functionId}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  fetchAnomalyByName(functionName) {
    const url = `/data/autocomplete/functionByName?name=${functionName}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * Note: these requests can fail silently and any empty response will fall back on defaults.
   * @method fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {RSVP.promise}
   */
  fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => checkStatus(res, 'get', true)),
      granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res => checkStatus(res, 'get', true))
    };
    return RSVP.hash(promiseHash);
  },

  /**
   * Loads time-series data into the anomaly-graph component.
   * Note: 'MINUTE' granularity loads 1 week of data. Otherwise, it loads 1 month.
   * @method triggerGraphFromMetric
   * @param {Number} metricId - Id of selected metric to graph
   * @return {undefined}
   */
  triggerGraphFromMetric() {
    const {
      maxTime,
      selectedFilters: filters,
      selectedDimension: dimension,
      selectedGranularity: granularity,
      selectedMetricOption: metric
    } = this.getProperties('maxTime', 'selectedFilters', 'selectedDimension', 'selectedGranularity', 'selectedMetricOption');

    // Use key properties to derive the metric data url
    const metricUrl = buildMetricDataUrl({ maxTime, filters, dimension, granularity, id: metric.id });

    // Fetch new graph metric data
    // TODO: const metricData = await fetch(metricUrl).then(checkStatus)
    fetch(metricUrl).then(checkStatus)
      .then(metricData => {
        this.setProperties({
          metricId: metric.id,
          isMetricSelected: true,
          isMetricDataLoading: false,
          showGraphLegend: true,
          selectedMetric: Object.assign(metricData, { color: 'blue' }),
          isMetricDataInvalid: !this.isMetricGraphable(metricData)
        });

        // Dimensions are selected. Compile, rank, and send them to the graph.
        if(dimension) {
          const orderedDimensions = getTopDimensions(metricData, this.get('dimensionCount'));
          // Update graph only if we have new dimension data
          if (orderedDimensions.length) {
            this.setProperties({
              isFetchingDimensions: false,
              isDimensionFetchDone: true,
              topDimensions: orderedDimensions,
              availableDimensions: orderedDimensions.length
            });
          }
        }
      }).catch((error) => {
        // The request failed. No graph to render.
        this.clearAll();
        this.setProperties({
          isMetricDataLoading: false,
          isMetricDataInvalid: true,
          selectMetricErrMsg: error
        });
      });
  },

  /**
   * Enriches the list of functions by Id, adding the properties we may want to display.
   * We are preparing to display the alerts that belong to the currently selected config group.
   * @method prepareFunctions
   * @param {Object} configGroup - the currently selected alert config group
   * @param {Object} newId - conditional param to help us tag any function that was "just added"
   * @return {RSVP.Promise} A new list of functions (alerts)
   */
  prepareFunctions(configGroup, newId = 0) {
    const newFunctionList = [];
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    let cnt = 0;

    // Build object for each function(alert) to display in results table
    return new RSVP.Promise((resolve) => {
      for (var functionId of existingFunctionList) {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push({
            number: cnt + 1,
            id: functionData.id,
            name: functionData.functionName,
            metric: functionData.metric + '::' + functionData.collection,
            type: functionData.type,
            active: functionData.isActive,
            isNewId: functionData.id === newId
          });
          cnt ++;
          if (existingFunctionList.length === cnt) {
            if (newId) {
              newFunctionList.reverse();
            }
            resolve(newFunctionList);
          }
        });
      }
    });
  },

  /**
   * If these two conditions are true, we assume the user wants to edit an existing alert group
   * @method isAlertGroupEditModeActive
   * @return {Boolean}
   */
  isAlertGroupEditModeActive: computed(
    'selectedConfigGroup',
    'newConfigGroupName',
    function() {
      return this.get('selectedConfigGroup') && isNone(this.get('newConfigGroupName'));
    }
  ),

  /**
   * Determines cases in which the filter field should be disabled
   * @method isFilterSelectDisabled
   * @return {Boolean}
   */
  isFilterSelectDisabled: computed(
    'filters',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || isEmpty(this.get('filters')));
    }
  ),

  /**
   * Determines cases in which the granularity field should be disabled
   * @method isGranularitySelectDisabled
   * @return {Boolean}
   */
  isGranularitySelectDisabled: computed(
    'granularities',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || isEmpty(this.get('granularities')));
    }
  ),

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @param {Number} metricId - Id of selected metric to graph
   * @return {Boolean} PreventSubmit
   */
  isSubmitDisabled: computed(
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    'isAlertNameDuplicate',
    'isGroupNameDuplicate',
    'isProcessingForm',
    function() {
      let isDisabled = false;
      const {
        requiredFields,
        isProcessingForm,
        newConfigGroupName,
        isAlertNameDuplicate,
        isGroupNameDuplicate,
        alertGroupNewRecipient,
        selectedConfigGroup: groupRecipients
      } = this.getProperties(
        'requiredFields',
        'isProcessingForm',
        'newConfigGroupName',
        'isAlertNameDuplicate',
        'isGroupNameDuplicate',
        'alertGroupNewRecipient',
        'selectedConfigGroup'
      );
      const hasRecipients = _.has(groupRecipients, 'recipients');
      // Any missing required field values?
      for (var field of requiredFields) {
        if (isBlank(this.get(field))) {
          isDisabled = true;
        }
      }
      // Enable submit if either of these field values are present
      if (isBlank(groupRecipients) && isBlank(newConfigGroupName)) {
        isDisabled = true;
      }
      // Duplicate alert Name or group name
      if (isAlertNameDuplicate || isGroupNameDuplicate) {
        isDisabled = true;
      }
      // For alert group email recipients, require presence only if group recipients is empty
      if (isBlank(alertGroupNewRecipient) && !hasRecipients) {
        isDisabled = true;
      }
      // Disable after submit clicked
      if (isProcessingForm) {
        isDisabled = true;
      }
      return isDisabled;
    }
  ),

  /**
   * Double-check new email array for errors.
   * @method isEmailValid
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean} whether errors were found
   */
  isEmailValid(emailArr) {
    const emailRegex = /^.{3,}@linkedin.com$/;
    let isValid = true;

    for (var email of emailArr) {
      if (!emailRegex.test(email)) {
        isValid = false;
      }
    }

    return isValid;
  },

  /**
   * Check for missing email address
   * @method isEmailPresent
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean}
   */
  isEmailPresent(emailArr) {
    let isEmailPresent = true;

    if (this.get('selectedConfigGroup') || this.get('newConfigGroupName')) {
      isEmailPresent = isPresent(this.get('selectedGroupRecipients')) || isPresent(emailArr);
    }

    return isEmailPresent;
  },

  /**
   * Auto-generate the alert name until the user directly edits it
   * @method modifyAlertFunctionName
   * @return {undefined}
   */
  modifyAlertFunctionName() {
    const {
      functionNamePrimer,
      isAlertNameUserModified
    } = this.getProperties('functionNamePrimer', 'isAlertNameUserModified');
    // If user has not yet edited the alert name, continue to auto-generate it.
    if (!isAlertNameUserModified) {
      this.set('alertFunctionName', functionNamePrimer);
    }
    // Each time we modify the name, we validate it as well to ensure no duplicates exist.
    this.send('validateAlertName', this.get('alertFunctionName'));
  },

  /**
   * Filter all existing alert groups down to only those that are active and belong to the
   * currently selected application team.
   * @method filteredConfigGroups
   * @param {Object} selectedApplication - user-selected application object
   * @return {Array} activeGroups - filtered list of groups that are active
   */
  filteredConfigGroups: computed(
    'selectedApplication',
    function() {
      const appName = this.get('selectedApplication');
      const activeGroups = this.get('allAlertsConfigGroups').filterBy('active');
      const groupsWithAppName = activeGroups.filter(group => isPresent(group.application));

      if (isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName));
      } else {
        return activeGroups;
      }
    }
  ),

  /**
   * Generate alert name primer based on user selections
   * @type {String}
   */
  functionNamePrimer: computed(
    'selectedPattern',
    'selectedDimension',
    'selectedGranularity',
    'selectedApplication',
    'selectedMetricOption',
    function() {
      const {
        selectedPattern,
        selectedDimension,
        selectedGranularity,
        selectedApplication,
        selectedMetricOption
      } = this.getProperties(
        'selectedPattern',
        'selectedDimension',
        'selectedGranularity',
        'selectedApplication',
        'selectedMetricOption'
      );
      const pattern = selectedPattern ? `${selectedPattern.camelize()}_` : '';
      const dimension = selectedDimension ? `${selectedDimension.camelize()}_` : '';
      const granularity = selectedGranularity ? selectedGranularity.toLowerCase().camelize() : '';
      const app = selectedApplication ? `${selectedApplication.camelize()}_` : 'applicationName_';
      const metric = selectedMetricOption ? `${selectedMetricOption.name.camelize()}_` : 'metricName_';
      return `${app}${metric}${dimension}${pattern}${granularity}`;
    }
  ),

  /**
   * Sets the message text over the graph placeholder before data is loaded
   * @method graphMessageText
   * @return {String} the appropriate graph placeholder text
   */
  graphMessageText: computed(
    'isMetricDataInvalid',
    function() {
      const defaultMsg = 'Once a metric is selected, the metric replay graph will show here';
      const invalidMsg = 'Sorry, metric has no current data';
      return this.get('isMetricDataInvalid') ? invalidMsg : defaultMsg;
    }
  ),

  /**
   * Preps a mailto link containing the currently selected metric name
   * @method graphMailtoLink
   * @return {String} the URI-encoded mailto link
   */
  graphMailtoLink: computed(
    'selectedMetricOption',
    function() {
      const selectedMetric = this.get('selectedMetricOption');
      const fullMetricName = `${selectedMetric.dataset}::${selectedMetric.name}`;
      const recipient = 'ask_thirdeye@linkedin.com';
      const subject = 'TE Self-Serve Create Alert Metric Issue';
      const body = `TE Team, please look into a possible inconsistency issue with [ ${fullMetricName} ]`;
      const mailtoString = `mailto:${recipient}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
      return mailtoString;
    }
  ),

  /**
   * Returns the appropriate subtitle for selected config group monitored alerts
   * @method selectedConfigGroupSubtitle
   * @return {String} title of expandable section for selected config group
   */
  selectedConfigGroupSubtitle: computed(
    'selectedConfigGroup',
    function () {
      return `Alerts Monitored by: ${this.get('selectedConfigGroup.name')}`;
    }
  ),

  /**
   * Builds the new alert settings to be sent to the alert creation task manager
   * @type {Object}
   */
  onboardFunctionPayload: computed(
    'alertFunctionName',
    'selectedMetricOption',
    'selectedDimension',
    'selectedFilters',
    'selectedGranularity',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    'selectedApplication',
    'alertFilterObj',
    function() {
      const {
        alertFunctionName: functionName,
        selectedMetricOption,
        selectedDimension,
        selectedFilters,
        selectedGranularity,
        selectedConfigGroup,
        newConfigGroupName,
        alertGroupNewRecipient,
        selectedApplication,
        alertFilterObj
      } = this.getProperties(
        'alertFunctionName',
        'selectedMetricOption',
        'selectedDimension',
        'selectedFilters',
        'selectedGranularity',
        'selectedConfigGroup',
        'newConfigGroupName',
        'alertGroupNewRecipient',
        'selectedApplication',
        'alertFilterObj'
      );

      const jobName = `${functionName}:${selectedMetricOption.id}`;
      const newAlertObj = {
        functionName,
        collection: selectedMetricOption.dataset,
        metric: selectedMetricOption.name,
        dataGranularity: selectedGranularity,
        pattern: alertFilterObj.pattern,
        application: selectedApplication
      };

      // Prepare config group property for new alert object and add it
      const isGroupExisting = selectedConfigGroup && isNone(newConfigGroupName);
      const subscriptionGroupKey = isGroupExisting ? 'alertId' : 'alertName';
      const subscriptionGroupValue = isGroupExisting ? selectedConfigGroup.id.toString() : newConfigGroupName;
      newAlertObj[subscriptionGroupKey] = subscriptionGroupValue;

      // Conditionally send recipients property
      if (alertGroupNewRecipient) {
        Object.assign(newAlertObj, { alertRecipients: alertGroupNewRecipient });
      }

      // Do we have custom sensitivity settings to add?
      if (alertFilterObj.isCustom) {
        Object.assign(newAlertObj, { features: alertFilterObj.features, mttd: alertFilterObj.mttd });
      }

      // Add filters property if present
      if (selectedFilters.length > 2) {
        Object.assign(newAlertObj, { filters: encodeURIComponent(selectedFilters) });
      }

      // Add dimensions if present
      if (selectedDimension) {
        Object.assign(newAlertObj, { exploreDimensions: selectedDimension });
      }

      // Add speedup prop for minutely metrics
      if (selectedGranularity.toLowerCase().includes('minute')) {
        Object.assign(newAlertObj, { speedup: true });
      }

      return {
        jobName,
        payload: JSON.stringify(newAlertObj)
      };

    }
  ),

  /**
   * Reset the form... clear all important fields
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      isFetchingDimensions: false,
      isDimensionFetchDone: false,
      isEmailError: false,
      isEmptyEmail: false,
      isFormDisabled: false,
      isMetricSelected: false,
      isDimensionError: false,
      isMetricDataInvalid: false,
      isSelectMetricError: false,
      selectedMetricOption: null,
      selectedPattern: null,
      selectedGranularity: null,
      selectedWeeklyEffect: true,
      selectedDimension: null,
      alertFunctionName: null,
      selectedAppName: null,
      selectedConfigGroup: null,
      newConfigGroupName: null,
      alertGroupNewRecipient: null,
      selectedGroupRecipients: null,
      isProcessingForm: false,
      isCreateGroupSuccess: false,
      isGroupNameDuplicate: false,
      isAlertNameDuplicate: false,
      graphEmailLinkProps: '',
      bsAlertBannerType: 'success',
      selectedFilters: JSON.stringify({}),
      selectedSeverityOption: 'Percentage of Change'
    });
    this.send('refreshModel');
  },

  /**
   * Actions for create alert form view
   */
  actions: {

    /**
     * Handles the primary metric selection in the alert creation
     */
    onPrimaryMetricToggle() {
      return;
    },

    /**
     * When a metric is selected, fetch its props, and send them to the graph builder
     * TODO: if 'hash.dimensions' is not needed, lets refactor the RSVP object instead of renaming
     * @method onSelectMetric
     * @param {Object} selectedObj - The selected metric
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      this.clearAll();
      this.setProperties({
        isMetricDataLoading: true,
        topDimensions: [],
        selectedMetricOption: selectedObj
      });
      this.fetchMetricData(selectedObj.id)
        .then((metricHash) => {
          const { maxTime, filters, dimensions, granularities } = metricHash;
          this.setProperties({
            maxTime,
            filters,
            dimensions,
            granularities,
            originalDimensions: dimensions,
            metricGranularityOptions: granularities,
            selectedGranularity: granularities[0],
            alertFunctionName: this.get('functionNamePrimer')
          });
          this.triggerGraphFromMetric();
        })
        .catch((err) => {
          this.setProperties({
            isSelectMetricError: true,
            selectMetricErrMsg: err
          });
        });
    },

    /**
     * When a filter is selected, fetch new anomaly graph data based on that filter
     * and trigger a new graph load. Also filter dimension names already selected as filters.
     * @method onSelectFilter
     * @param {Object} selectedFilters - The selected filters to apply
     * @return {undefined}
     */
    onSelectFilter(selectedFilters) {
      const selectedFilterObj = JSON.parse(selectedFilters);
      const dimensionNameSet = new Set(this.get('originalDimensions'));
      const filterNames = Object.keys(JSON.parse(selectedFilters));
      let isSelectedDimensionEqualToSelectedFilter = false;

      // Remove selected filters from dimension options only if filter has single entity
      for (var key of filterNames) {
        if (selectedFilterObj[key].length === 1) {
          dimensionNameSet.delete(key);
          if (key === this.get('selectedDimension')) {
            isSelectedDimensionEqualToSelectedFilter = true;
          }
        }
      }
      // Update dimension options and loader
      this.setProperties({
        dimensions: [...dimensionNameSet],
        isMetricDataLoading: true
      });
      // Do not allow selected dimension to match selected filter
      if (isSelectedDimensionEqualToSelectedFilter) {
        this.set('selectedDimension', 'All');
      }
      // Fetch new graph data with selected filters
      this.triggerGraphFromMetric();
    },

    /**
     * When a dimension is selected, fetch new anomaly graph data based on that dimension
     * and trigger a new graph load, showing the top contributing subdimensions.
     * @method onSelectFilter
     * @param {Object} selectedDimension - The selected dimension to apply
     * @return {undefined}
     */
    onSelectDimension(selectedDimension) {
      this.setProperties({
        selectedDimension,
        isDimensionFetchDone: false
      });
      if (selectedDimension === 'All') {
        this.setProperties({
          topDimensions: [],
          isFetchingDimensions: false
        });
      } else {
        this.setProperties({
          isMetricDataLoading: true,
          isFetchingDimensions: true
        });
        this.modifyAlertFunctionName();
        this.triggerGraphFromMetric();
      }
    },

    /**
     * Set our selected granularity. Trigger graph reload.
     * @method onSelectGranularity
     * @param {Object} selectedObj - The selected granularity option
     * @return {undefined}
     */
    onSelectGranularity(selectedObj) {
      this.setProperties({
        selectedGranularity: selectedObj,
        isMetricDataLoading: true
      });
      this.modifyAlertFunctionName();
      this.triggerGraphFromMetric();
    },

    /**
     * Set selected pattern and trigger auto-generation of Alert Name
     * @method onSelectPattern
     * @param {Object} selectedOption - The selected pattern option
     * @return {undefined}
     */
    onSelectPattern(selectedOption) {
      this.set('selectedPattern', selectedOption);
      this.modifyAlertFunctionName();
    },

    /**
     * Set our selected application name
     * @method onSelectAppName
     * @param {Object} selectedObj - The selected app name option
     * @return {undefined}
     */
    onSelectAppName(selectedObj) {
      this.setProperties({
        selectedAppName: selectedObj,
        selectedApplication: selectedObj.application
      });
      this.modifyAlertFunctionName();
    },

    /**
     * Set our selected alert configuration group. If one is selected, display editable fields
     * for that group and display the list of functions that belong to that group.
     * @method onSelectConfigGroup
     * @param {Object} selectedObj - The selected config group option
     * @return {undefined}
     */
    onSelectConfigGroup(selectedObj) {
      const emails = selectedObj.recipients || '';
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        isEmptyEmail: isEmpty(emails),
        selectedGroupRecipients: emails.split(',').filter(e => String(e).trim()).join(', ')
      });
      this.prepareFunctions(selectedObj).then(functionData => {
        this.set('selectedGroupFunctions', functionData);
      });
    },

    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} name - The new alert name
     * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
     * If user-modified, we will stop modifying it dynamically (via 'isAlertNameUserModified')
     * @return {undefined}
     */
    validateAlertName(name, userModified = false) {
      let isDuplicateName = false;
      this.fetchAnomalyByName(name).then(anomaly => {
        for (var resultObj of anomaly) {
          if (resultObj.functionName === name) {
            isDuplicateName = true;
          }
        }
        // If the user edits the alert name, we want to stop auto-generating it.
        if (userModified) {
          this.set('isAlertNameUserModified', true);
        }
        // Either add or clear the "is duplicate name" banner
        this.set('isAlertNameDuplicate', isDuplicateName);
      });
    },

    /**
     * Reset selected group list if user chooses to create a new group
     * @method validateNewGroupName
     * @param {String} name - User-provided alert group name
     * @return {undefined}
     */
    validateNewGroupName(name) {
      this.set('isGroupNameDuplicate', false);
      // return early if name is empty
      if (!name || !name.trim().length) { return; }
      const nameExists = this.get('allAlertsConfigGroups')
        .map(group => group.name)
        .includes(name);

      // set error message and return early if group name exists
      if (nameExists) {
        this.set('isGroupNameDuplicate', true);
        return;
      }

      this.setProperties({
        newConfigGroupName: name,
        selectedConfigGroup: null,
        selectedGroupRecipients: null,
        isEmptyEmail: isEmpty(this.get('alertGroupNewRecipient'))
      });
    },

    /**
     * Verify that email address does not already exist in alert group. If it does, remove it and alert user.
     * @method validateAlertEmail
     * @param {String} emailInput - Comma-separated list of new emails to add to the config group.
     * @return {undefined}
     */
    validateAlertEmail(emailInput) {
      const newEmailArr = emailInput.replace(/\s+/g, '').split(',');
      let existingEmailArr = this.get('selectedGroupRecipients');
      let cleanEmailArr = [];
      let badEmailArr = [];
      let isDuplicateEmail = false;

      // Release submit button error state
      this.setProperties({
        isEmailError: false,
        isProcessingForm: false,
        isEditedConfigGroup: true,
        isEmptyEmail: isPresent(this.get('newConfigGroupName')) && !emailInput.length
      });

      // Check for duplicates
      if (emailInput.trim() && existingEmailArr) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (email.length && existingEmailArr.includes(email)) {
            isDuplicateEmail = true;
            badEmailArr.push(email);
          } else {
            cleanEmailArr.push(email);
          }
        }
        this.setProperties({
          isDuplicateEmail,
          duplicateEmails: badEmailArr.join()
        });
      }
    },

    /**
     * Reset the form... clear all important fields
     * @method clearAll
     * @return {undefined}
     */
    onResetForm() {
      this.clearAll();
    },

    /**
     * Enable reaction to dimension toggling in graph legend component
     * @method onSelection
     * @return {undefined}
     */
    onSelection(selectedDimension) {
      const { isSelected } = selectedDimension;
      set(selectedDimension, 'isSelected', !isSelected);
    },

    /**
     * Check for email errors before triggering onboarding job
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      const {
        metricId,
        selectedMetric,
        isDuplicateEmail,
        onboardFunctionPayload,
        alertGroupNewRecipient: newEmails
      } = this.getProperties('metricId', 'selectedMetric', 'isDuplicateEmail', 'onboardFunctionPayload', 'alertGroupNewRecipient');
      const newEmailsArr = newEmails ? newEmails.replace(/ /g, '').split(',') : [];
      const isEmailError = !this.isEmailValid(newEmailsArr);

      // TODO: cache latest selected metric data using session storage
      // setMetricData(metricId, selectedMetric);

      // Update validation properties
      this.setProperties({
        isEmailError,
        isProcessingForm: true,
        isEmptyEmail: !this.isEmailPresent(newEmailsArr)
      });

      // Exit quietly (showing warning) in the event of error
      if (isEmailError || isDuplicateEmail) { return; }

      // Begin onboarding tasks
      this.send('triggerOnboardingJob', onboardFunctionPayload);
    }
  }
});
