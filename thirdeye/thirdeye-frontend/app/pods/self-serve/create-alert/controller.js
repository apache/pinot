/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import { reads } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import RSVP from "rsvp";
import fetch from 'fetch';
import moment from 'moment';
import Controller from '@ember/controller';
import { computed, set, get, getWithDefault } from '@ember/object';
import { task, timeout } from 'ember-concurrency';
import {
  isPresent,
  isEmpty,
  isNone,
  isBlank
} from "@ember/utils";
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import {toastOptions} from 'thirdeye-frontend/utils/constants';
import {
  selfServeApiGraph,
  autocompleteAPI
} from 'thirdeye-frontend/utils/api/self-serve';
import {
  buildMetricDataUrl,
  formatConfigGroupProps,
  getTopDimensions
} from 'thirdeye-frontend/utils/manage-alert-utils';
import config from 'thirdeye-frontend/config/environment';

export default Controller.extend({
  notifications: service('toast'),

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
  isSecondaryDataLoading: false,
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
  selectedTuneType: 'current',
  graphEmailLinkProps: '',
  dimensionCount: 7,
  availableDimensions: 0,
  metricLookupCache: [],
  filtersCache: {},
  dimensionsCache: [],
  noResultsArray: [{
    value: 'abcdefghijklmnopqrstuvwxyz0123456789',
    caption: 'No Results',
    snippet: ''
  }],
  metricHelpMailto: `mailto:${config.email}?subject=Metric Onboarding Request (non-additive UMP or derived)`,
  helpDocLink: config.docs ? config.docs.createAlert : null,

  /**
   * Component property initial settings
   */
  filters: {},
  graphConfig: {},
  selectedFilters: JSON.stringify({}),
  selectedWeeklyEffect: true,
  isForm: false,
  toggleCollapsed: true,              // flag for the accordion that hides/shows preview
  detectionYaml: null,                // The YAML for the anomaly detection
  subscriptionYaml:  null,            // The YAML for the subscription group
  alertDataIsCurrent: true,
  disableYamlSave: true,
  detectionError: false,
  detectionErrorMsg: null,
  detectionErrorInfo: null,
  detectionErrorScroll: false,
  previewError: false,
  previewErrorMsg: null,
  previewErrorInfo: null,
  previewErrorScroll: false,



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
   * Options for patterns of interest field. These may eventually load from the backend.
   */
  patternsOfInterest: ['Higher or lower than expected', 'Higher than expected', 'Lower than expected'],

  /**
   * Mapping user readable pattern and sensitivity to DB values
   */
  optionMap: {
    pattern: {
      'Higher or lower than expected': 'UP,DOWN',
      'Higher than expected': 'UP',
      'Lower than expected': 'DOWN'
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
  allApplicationNames: reads('model.applications'),

  /**
   * The list of all existing alert configuration groups.
   */
  allAlertsConfigGroups: reads('model.subscriptionGroups'),

  /**
   * The debug flag
   */
  debug: reads('model.debug'),

  /**
   * Handler for search by function name - using ember concurrency (task)
   * @method searchMetricsList
   * @param {metric} String - portion of metric name used in typeahead
   * @return {Promise}
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const autoCompleteResults = yield fetch(autocompleteAPI.metric(metric)).then(checkStatus);
    this.get('metricLookupCache').push(...autoCompleteResults);
    return autoCompleteResults;
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
    const url = autocompleteAPI.getAlertById(functionId);
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  fetchAlertsByName(functionName) {
    const url = autocompleteAPI.alertByName(functionName);
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
      maxTime: fetch(selfServeApiGraph.maxDataTime(metricId)).then(res => checkStatus(res, 'get', true)),
      granularities: fetch(selfServeApiGraph.metricGranularity(metricId)).then(res => checkStatus(res, 'get', true)),
      filters: fetch(selfServeApiGraph.metricFilters(metricId)).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(selfServeApiGraph.metricDimensions(metricId)).then(res => checkStatus(res, 'get', true))
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
    const metricUrl = buildMetricDataUrl({ maxTime, filters, dimension: 'All', granularity, id: metric.id }); // NOTE: avoid dimension explosion - dimension

    // Fetch new graph metric data
    // TODO: const metricData = await fetch(metricUrl).then(checkStatus)
    fetch(metricUrl).then(checkStatus)
      .then(metricData => {
        const isDataGood = this.isMetricGraphable(metricData);
        this.setProperties({
          metricId: metric.id,
          isMetricSelected: true,
          isMetricDataLoading: false,
          isSecondaryDataLoading: false,
          showGraphLegend: true,
          selectedMetric: Object.assign(metricData, { color: 'blue' }),
          isMetricDataInvalid: !isDataGood
        });

        // Dimensions are selected. Compile, rank, and send them to the graph.
        if(dimension) {
          const orderedDimensions = getTopDimensions(metricData, this.get('dimensionCount'));
          // Update graph only if we have new dimension data
          if (orderedDimensions.length) {
            this.setProperties({
              isDimensionFetchDone: true,
              topDimensions: orderedDimensions,
              availableDimensions: orderedDimensions.length
            });
          }
        }
      }).catch((error) => {
        this.setProperties({
          isMetricDataInvalid: true,
          isMetricDataLoading: false,
          isSecondaryDataLoading: false,
          selectMetricErrMsg: error
        });
      });
  },

  /**
   * Enriches the list of functions by Id, adding the properties we may want to display.
   * We are preparing to display the alerts that belong to the currently selected config group.
   * TODO: Good candidate to move to self-serve shared services
   * @method prepareFunctions
   * @param {Object} configGroup - the currently selected alert config group
   * @return {RSVP.Promise} A new list of functions (alerts)
   */
  prepareFunctions(configGroup) {
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    const newFunctionList = [];
    let cnt = 0;

    // Build object for each function(alert) to display in results table
    return new RSVP.Promise((resolve) => {
      existingFunctionList.forEach((functionId) => {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push(formatConfigGroupProps(functionData));
          cnt ++;
          if (existingFunctionList.length === cnt) {
            resolve(newFunctionList);
          }
        });
      });
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
   * Allows us to enable/disable the custom tuning options
   * @type {Boolean}
   */
  isCustomFieldsDisabled: computed(
    'selectedTuneType',
    'isMetricSelected',
    function() {
      const isEnabled = this.get('selectedTuneType') === 'custom' && this.get('isMetricSelected');
      return !isEnabled;
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
    'isDuplicateEmail',
    'isEmptyEmail',
    'isEmailError',
    'isProcessingForm',
    function() {
      let isDisabled = false;
      const {
        requiredFields,
        isProcessingForm,
        isDuplicateEmail,
        isEmptyEmail,
        isEmailError,
        newConfigGroupName,
        isAlertNameDuplicate,
        isGroupNameDuplicate,
        alertGroupNewRecipient,
        selectedConfigGroup: groupRecipients
      } = this.getProperties(
        'requiredFields',
        'isProcessingForm',
        'isDuplicateEmail',
        'isEmptyEmail',
        'isEmailError',
        'newConfigGroupName',
        'isAlertNameDuplicate',
        'isGroupNameDuplicate',
        'alertGroupNewRecipient',
        'selectedConfigGroup'
      );
      const existingRecipients = groupRecipients ? getWithDefault(groupRecipients, 'receiverAddresses.to', []) : [];
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
      if (isAlertNameDuplicate || isGroupNameDuplicate || isDuplicateEmail || isEmptyEmail || isEmailError) {
        isDisabled = true;
      }
      // For alert group email recipients, require presence only if group recipients is empty
      if (isBlank(alertGroupNewRecipient) && !existingRecipients.length) {
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
      isEmailPresent = isPresent(this.get('selectedGroupToRecipients')) || isPresent(emailArr);
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
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName.toLowerCase()));
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
        optionMap,
        selectedPattern,
        selectedDimension,
        selectedGranularity,
        selectedApplication,
        selectedMetricOption
      } = this.getProperties(
        'optionMap',
        'selectedPattern',
        'selectedDimension',
        'selectedGranularity',
        'selectedApplication',
        'selectedMetricOption'
      );
      const pattern = selectedPattern ? optionMap.pattern[selectedPattern] : null;
      const formattedPattern = pattern ? `${pattern.toLowerCase().replace(',', ' ').camelize()}_` : '';
      const dimension = selectedDimension ? `${selectedDimension.camelize()}_` : '';
      const granularity = selectedGranularity ? selectedGranularity.toLowerCase().camelize() : '';
      const app = selectedApplication ? `${selectedApplication.camelize()}_` : 'applicationName_';
      const metric = selectedMetricOption ? `${selectedMetricOption.name.camelize()}_` : 'metricName_';
      return `${app}${metric}${dimension}${formattedPattern}${granularity}`;
    }
  ),

  /**
   * Determines whether input fields in general are enabled. When metric data is 'invalid',
   * we will still enable alert creation.
   * @method generalFieldsEnabled
   * @return {Boolean}
   */
  generalFieldsEnabled: computed.or('isMetricSelected', 'isMetricDataInvalid'),

  /**
   * Preps a mailto link containing the currently selected metric name
   * @method graphMailtoLink
   * @return {String} the URI-encoded mailto link
   */
  graphMailtoLink: computed(
    'selectedMetricOption',
    function() {
      const recipient = config.email;
      const selectedMetric = this.get('selectedMetricOption');
      const subject = 'TE Self-Serve Create Alert Metric Issue';
      const fullMetricName = selectedMetric ? `${selectedMetric.dataset}::${selectedMetric.name}` : '';
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
      return `See all alerts monitored by: ${this.get('selectedConfigGroup.name')}`;
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

      // Construct a unique-enough job name for this alert
      const jobName = `${functionName}:${selectedMetricOption.id}${moment().valueOf()}`;
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
        Object.assign(newAlertObj, { alertRecipients: alertGroupNewRecipient.replace(/ /g, '') });
      }

      // Do we have custom sensitivity settings to add?
      if (alertFilterObj.isCustom) {
        Object.assign(newAlertObj, { tuningFeatures: alertFilterObj.features, mttd: alertFilterObj.mttd });
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
        Object.assign(newAlertObj, { speedup: 'true' });
      }

      return {
        jobName,
        payload: newAlertObj
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
      isSecondaryDataLoading: false,
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
      selectedGroupToRecipients: null,
      selectedGroupBccRecipients: null,
      selectedGroupCcRecipients: null,
      isProcessingForm: false,
      isCreateGroupSuccess: false,
      isGroupNameDuplicate: false,
      isAlertNameDuplicate: false,
      graphEmailLinkProps: '',
      bsAlertBannerType: 'success',
      selectedFilters: JSON.stringify({})
    });
    this.send('refreshModel');
  },

  /**
   * Actions for create alert form view
   */
  actions: {
    changeAccordion() {
      set(this, 'toggleCollapsed', !get(this, 'toggleCollapsed'));
    },

    /**
     * Clears YAML content, disables 'save changes' button, and moves to form
     */
    cancelAlertYaml() {
      set(this, 'isForm', true);
      set(this, 'currentMetric', null);
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
        topDimensions: [],
        isMetricDataLoading: true,
        selectedMetricOption: selectedObj
      });
      this.fetchMetricData(selectedObj.id)
        .then((metricHash) => {
          const { maxTime, filters, dimensions, granularities } = metricHash;
          const targetMetric = this.get('metricLookupCache').find(metric => metric.id === selectedObj.id);
          const inGraphLink = getWithDefault(targetMetric, 'extSourceLinkInfo.INGRAPH', null);
          // In the event that we have an "ingraph" metric, enable only "minute" level granularity
          const adjustedGranularity = (targetMetric && inGraphLink) ? granularities.filter(g => g.toLowerCase().includes('minute')) : granularities;

          this.setProperties({
            maxTime,
            filters,
            dimensions,
            metricLookupCache: [],
            granularities: adjustedGranularity,
            originalDimensions: dimensions,
            metricGranularityOptions: adjustedGranularity,
            selectedGranularity: adjustedGranularity[0],
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
        isSecondaryDataLoading: true
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
          isSecondaryDataLoading: false
        });
      } else {
        this.set('isSecondaryDataLoading', true);
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
        isSecondaryDataLoading: true
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
      const toAddr = ((selectedObj.receiverAddresses || []).to || []).join(", ");
      const ccAddr = ((selectedObj.receiverAddresses || []).cc || []).join(", ");
      const bccAddr = ((selectedObj.receiverAddresses || []).bcc || []).join(", ");
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        isEmptyEmail: isEmpty(toAddr),
        selectedGroupToRecipients: toAddr,
        selectedGroupCcRecipients: ccAddr,
        selectedGroupBccRecipients: bccAddr
      });
      this.prepareFunctions(selectedObj).then(functionData => {
        this.set('selectedGroupFunctions', functionData);
      });
    },

    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} userProvidedName - The new alert name
     * @param {Boolean} userModified - Up to this moment, is the new name auto-generated, or user modified?
     * If user-modified, we will stop modifying it dynamically (via 'isAlertNameUserModified')
     * @return {undefined}
     */
    validateAlertName(userProvidedName, userModified = false) {
      this.fetchAlertsByName(userProvidedName).then(matchingAlerts => {
        const isDuplicateName = matchingAlerts.find(alert => alert.functionName === userProvidedName);
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
        selectedGroupToRecipients: null,
        selectedGroupCcRecipients: null,
        selectedGroupBccRecipients: null,
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
      let existingEmailArr = this.get('selectedGroupToRecipients');
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
          duplicateEmails: badEmailArr.join(", ")
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
     * update the detection yaml string
     * @method updateDetectionYaml
     * @return {undefined}
     */
    updateDetectionYaml(updatedYaml) {
      this.setProperties({
        detectionYaml: updatedYaml,
        alertDataIsCurrent: false,
        disableYamlSave: false
      });
    },

    /**
     * set preview error for pushing down to detection-yaml component
     * @method setPreviewError
     * @return {undefined}
     */
    setPreviewError(bubbledObject) {
      this.setProperties({
        previewError: bubbledObject.previewError,
        previewErrorMsg: bubbledObject.previewErrorMsg,
        previewErrorInfo: bubbledObject.previewErrorInfo,
        previewErrorScroll: bubbledObject.previewError
      });
    },

    /**
     * set property value to false
     * @method resetErrorScroll
     * @param {string} propertyName - name of property to reset (ie 'detectionErrorScroll')
     * @return {undefined}
     */
    resetErrorScroll(propertyName) {
      set(this, propertyName, false);
    },

    /**
     * update the subscription yaml string
     * @method updateSubscriptionYaml
     * @return {undefined}
     */
    updateSubscriptionYaml(updatedYaml) {
      set(this, 'subscriptionYaml', updatedYaml);
    },

    /**
     * update the subscription group object for dropdown
     * @method updateSubscriptionGroup
     * @return {undefined}
     */
    changeSubscriptionGroup(group) {
      this.setProperties({
        subscriptionYaml: group.yaml,
        groupName: group
      });
    },

    /**
     * Fired by create button in YAML UI
     * Grabs YAML content and sends it
     */
    createAlertYamlAction() {
      set(this, 'detectionError', false);
      const content = {
        detection: get(this, 'detectionYaml'),
        subscription: get(this, 'subscriptionYaml')
      };
      const url = '/yaml/create-alert';
      const postProps = {
        method: 'post',
        body: JSON.stringify(content),
        headers: { 'content-type': 'application/json' }
      };
      const notifications = get(this, 'notifications');

      fetch(url, postProps).then((res) => {
        res.json().then((result) => {
          if(result){
            if (result.subscriptionConfigId && result.detectionConfigId) {
              notifications.success('Created alert successfully.', 'Created', toastOptions);
              this.transitionToRoute('manage.explore', result.detectionConfigId);
            } else {
              notifications.error(result.message, 'Error', toastOptions);
              this.setProperties({
                detectionError: true,
                detectionErrorMsg: result.message,
                detectionErrorInfo: result["more-info"],
                detectionErrorScroll: true
              });
            }
          }
        });
      }).catch((error) => {
        notifications.error('Create alert failed.', error, toastOptions);
        this.setProperties({
          detectionError: true,
          detectionErrorMsg: 'Create alert failed.',
          detectionErrorInfo: error,
          detectionErrorScroll: true
        });
      });
    },

    /**
     * Check for email errors before triggering onboarding job
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      const {
        isDuplicateEmail,
        onboardFunctionPayload,
        alertGroupNewRecipient: newEmails
      } = this.getProperties('isDuplicateEmail', 'onboardFunctionPayload', 'alertGroupNewRecipient');
      const newEmailsArr = newEmails ? newEmails.replace(/ /g, '').split(',') : [];
      const isEmailError = !this.isEmailValid(newEmailsArr);

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
