/**
 * Controller for Alert Details Page: Tune Sensitivity Tab
 * @module manage/alert/tune
 * @exports manage/alert/tune
 */
import _ from 'lodash';
import moment from 'moment';
import Controller from '@ember/controller';
import { later } from '@ember/runloop';
import { isPresent } from "@ember/utils";
import { computed, set, get, getProperties, setProperties } from '@ember/object';
import { inject as service } from '@ember/service';
import { buildDateEod } from 'thirdeye-frontend/utils/utils';
import { anomalyResponseMap } from 'thirdeye-frontend/utils/anomaly';
import { buildAnomalyStats } from 'thirdeye-frontend/utils/manage-alert-utils';

export default Controller.extend({
  /**
   * Be ready to receive time span for anomalies via query params
   */
  queryParams: ['duration', 'startDate', 'endDate'],
  duration: null,
  startDate: null,
  endDate: null,

  /**
   * Make duration service accessible
   */
  durationCache: service('services/duration'),

  /**
   * Make toast service accessible
   */
  notifications: service('toast'),

  /**
   * Set initial view values
   * @method initialize
   * @return {undefined}
   */
  initialize() {
    this.setProperties({
      filterBy: 'All',
      isGraphReady: false,
      isTunePreviewActive: false,
      isTuneSaveSuccess: false,
      isTuneSaveFailure: false,
      selectedTuneType: 'current',
      predefinedRanges: {},
      today: moment(),
      selectedSortMode: '',
      activeRangeStart: '',
      activeRangeEnd: '',
      removedAnomalies: 0,
      sortColumnStartUp: false,
      sortColumnScoreUp: false,
      sortColumnChangeUp: false,
      isAnomalyTableLoading: false,
      sortColumnResolutionUp: false,
      isPerformanceDataLoading: false,
      customMttdClasses: 'form-control te-input'
    });
  },

  /**
   * Severity power-select options
   * @type {Array}
   */
  tuneSeverityOptions: computed('severityMap', function() {
    return Object.keys(this.get('severityMap'));
  }),

  /**
   * Returns selectable pattern options for power-select
   * @type {Array}
   */
  tunePatternOptions: computed('patternMap', function() {
    return Object.keys(this.get('patternMap'));
  }),

  /**
   * Mapping anomaly table column names to corresponding prop keys
   */
  sortMap: {
    start: 'anomalyStart',
    score: 'severityScore',
    change: 'changeRate',
    resolution: 'anomalyFeedback'
  },

  /**
   * Conditional formatting for tuning fields
   * @type {Boolean}
   */
  isTuneAmountPercent: computed('selectedSeverityOption', function() {
    return this.get('selectedSeverityOption') !== 'Absolute Value of Change';
  }),

  /**
   * Builds the new autotune filter from custom tuning options
   * @type {String}
   */
  customTuneQueryString: computed(
    'selectedSeverityOption',
    'customPercentChange',
    'customMttdChange',
    'selectedTunePattern',
    function() {
      const {
        severityMap,
        patternMap,
        customPercentChange: amountChange,
        selectedTunePattern: selectedPattern,
        selectedSeverityOption: selectedSeverity
      } = this.getProperties('severityMap', 'patternMap', 'customPercentChange', 'selectedTunePattern', 'selectedSeverityOption');
      const isPercent = selectedSeverity === 'Percentage of Change';
      const mttdVal = Number(this.get('customMttdChange')).toFixed(2);
      const severityThresholdVal = isPercent ? (Number(amountChange)/100).toFixed(2) : amountChange;
      const featureString = `window_size_in_hour,${severityMap[selectedSeverity]}`;
      const mttdString = `window_size_in_hour=${mttdVal};${severityMap[selectedSeverity]}=${severityThresholdVal}`;
      const patternString = patternMap[selectedPattern] ? `&pattern=${encodeURIComponent(patternMap[selectedPattern])}` : '';
      const configString = `&tuningFeatures=${encodeURIComponent(featureString)}&mttd=${encodeURIComponent(mttdString)}${patternString}`;
      return { configString, severityVal: severityThresholdVal };
    }
  ),

  /**
   * Indicates the allowed date range picker increment based on granularity
   * @type {Number}
   */
  timePickerIncrement: computed('alertData.windowUnit', function() {
    const granularity = this.get('alertData.windowUnit').toLowerCase();

    switch(granularity) {
      case 'days':
        return 1440;
      case 'hours':
        return 60;
      default:
        return 5;
    }
  }),

  /**
   * Allows us to enable/disable the custom tuning options
   * @type {Boolean}
   */
  isCustomFieldsDisabled: computed('selectedTuneType', function() {
    return this.get('selectedTuneType') === 'current';
  }),

  /**
   * date-time-picker: indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: computed('alertData.windowUnit', function() {
    const granularity = this.get('alertData.windowUnit').toLowerCase();

    switch(granularity) {
      case 'days':
        return 'MMM D, YYYY';
      case 'hours':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  /**
   * Data needed to render the stats 'cards' above the anomaly graph for this alert
   * NOTE: buildAnomalyStats util currently requires both 'current' and 'projected' props to be present.
   * @type {Object}
   */
  anomalyStats: computed(
    'alertEvalMetrics',
    'isTuneAmountPercent',
    'customPercentChange',
    'alertEvalMetrics.projected',
    function() {
      const {
        isTuneAmountPercent,
        alertEvalMetrics: metrics,
        customPercentChange: severity
      } = this.getProperties(
        'isTuneAmountPercent',
        'alertEvalMetrics',
        'customPercentChange'
      );
      const severityUnit = isTuneAmountPercent ? '%' : '';
      const isPerfDataReady = _.has(metrics, 'current');
      const statsCards = [
        {
          title: 'Estimated number of anomalies',
          key: 'totalAlerts',
          tooltip: false,
          text: 'Estimated number of anomalies  based on alert settings'
        },
        {
          title: 'Estimated precision',
          key: 'precision',
          units: '%',
          tooltip: false,
          text: 'Among all anomalies sent by the alert, the % of them that are true.'
        },
        {
          title: 'Estimated recall',
          key: 'recall',
          units: '%',
          tooltip: false,
          text: 'Among all anomalies that happened, the % of them sent by the alert.'
        },
        {
          title: `MTTD for > ${severity}${severityUnit} change`,
          key: 'mttd',
          units: 'hrs',
          tooltip: false,
          text: `Minimum time to detect for anomalies with > ${severity}${severityUnit} change`
        }
      ];

      return isPerfDataReady ? buildAnomalyStats(metrics, statsCards, false) : [];
    }
  ),

  /**
   * Data needed to render the stats 'cards' above the anomaly graph for this alert
   * @type {Object}
   */
  diffedAnomalies: computed(
    'anomalyData',
    'filterBy',
    'selectedSortMode',
    function() {
      const {
        anomalyData: anomalies,
        filterBy: activeFilter,
        selectedSortMode
      } = this.getProperties('anomalyData', 'filterBy', 'selectedSortMode');
      let filterKey = '';
      let filteredAnomalies = anomalies || [];
      let num = 1;

      switch (activeFilter) {
        case 'True Anomalies':
          filterKey = 'True Anomaly';
          break;
        case 'False Alarms':
          filterKey = 'False Alarm';
          break;
        case 'User Reported':
          filterKey = 'New Trend';
          break;
        default:
          filterKey = '';
      }

      // Filter anomalies in table according to filterkey
      if (activeFilter !== 'All') {
        filteredAnomalies = anomalies.filter(anomaly => anomaly.anomalyFeedback === filterKey);
      }
      if (selectedSortMode) {
        let [ sortKey, sortDir ] = selectedSortMode.split(':');
        if (sortDir === 'up') {
          filteredAnomalies = filteredAnomalies.sortBy(this.get('sortMap')[sortKey]);
        } else {
          filteredAnomalies = filteredAnomalies.sortBy(this.get('sortMap')[sortKey]).reverse();
        }
      }

      // Number the list
      filteredAnomalies.forEach((anomaly) => {
        set(anomaly, 'index', num);
        setProperties(anomaly, {
          index: num,
          feedbackLabel: anomalyResponseMap[anomaly.anomalyFeedback] || anomaly.anomalyFeedback
        });
        num++;
      });

      return filteredAnomalies;
    }
  ),

  /**
   * Reset the controller values on exit
   * @method clearAll
   */
  clearAll() {
    this.setProperties({
      alertEvalMetrics: {}
    });
  },

  actions: {

    /**
     * This field will not accept empty input - default back to the original value
     * @method onChangeSeverityValue
     * @param {String} severity - the custom tuning severity input
     */
    onChangeSeverityValue(severity) {
      if (!isPresent(severity)) {
        this.set('customPercentChange', this.model.customPercentChange);
      }
    },

    /**
     * This field will not accept empty input - default back to the original value
     * @method onChangeMttdValue
     * @param {String} mttd - the custom tuning mttd input
     */
    onChangeMttdValue(mttd) {
      if (!isPresent(mttd)) {
        this.set('customMttdChange', this.model.customMttdChange);
      }
    },

    /**
     * Sets the new custom date range for anomaly coverage
     * @method onRangeSelection
     * @param {Object} rangeOption - the user-selected time range to load
     */
    onRangeSelection(rangeOption) {
      const {
        start,
        end,
        value: duration
      } = rangeOption;
      const durationObj = {
        duration,
        startDate: moment(start).valueOf(),
        endDate: moment(end).valueOf()
      };
      // Cache the new time range and update page with it
      this.get('durationCache').setDuration(durationObj);
      this.transitionToRoute({ queryParams: durationObj });
    },

    /**
     * Save the currently loaded tuning options
     */
    onSubmitTuning() {
      this.send('submitTuningRequest', this.get('autoTuneId'));
    },

    /**
     * Handle "reset" click - reload the model
     */
    onResetPage() {
      this.initialize();
      this.set('alertEvalMetrics.projected', this.get('originalProjectedMetrics'));
      this.send('resetTuningParams', this.get('alertData'));
    },

    /**
     * Replaces the 'tableStats' object with a new one with selected filter
     * activated and triggers table filtering.
     * @param {String} metric - label of the currently selected category
     */
    toggleCategory(metric) {
      const stats = this.get('tableStats');
      const newStats = stats.map((cat) => {
        return {
          count: cat.count,
          label: cat.label,
          isActive: false
        };
      });
      // Activate selected metric in our new stats object
      newStats.find(cat => cat.label === metric).isActive = true;
      // Apply new table stats object and trigger re-render of filtered anomalies
      this.setProperties({
        tableStats: newStats,
        filterBy: metric
      });
    },

    /**
     * Handle sorting for each sortable table column
     * @param {String} sortKey  - stringified start date
     */
    toggleSortDirection(sortKey) {
      const propName = `sortColumn${sortKey.capitalize()}Up` || '';

      this.toggleProperty(propName);
      if (this.get(propName)) {
        this.set('selectedSortMode', `${sortKey}:up`);
      } else {
        this.set('selectedSortMode', `${sortKey}:down`);
      }
      // On sort, set table to first pagination page
      this.set('currentPage', 1);
    },

    /**
     * On "preview" click, display the resulting anomaly table and trigger
     * tuning if we have custom settings (tuning data for default option is already loaded)
     */
    onClickPreviewPerformance() {
      const defaultConfig = { configString: '' };
      const { customMttdClasses, mttdMinimums, alertData, notifications, customMttdChange } = getProperties(this,
        'customMttdClasses',
        'mttdMinimums',
        'alertData',
        'notifications',
        'customMttdChange'
      );
      const granularityBucket = alertData.bucketUnit ? alertData.bucketUnit.toLowerCase() : null;
      const isBucketDefaultPresent = granularityBucket && mttdMinimums.hasOwnProperty(granularityBucket);
      const isMttdSetTooLow = isBucketDefaultPresent ? Number(customMttdChange) < Number(mttdMinimums[granularityBucket]) : false;
      const currentMinimumMttd = mttdMinimums[granularityBucket];
      const mttdUnit = granularityBucket === 'hours' ? 'hour' : 'hours';
      const mttdErrMsg = `MTTD is set too low for metric granularity. Please enter a value of at least ${currentMinimumMttd} ${mttdUnit}.`;
      const toastOptions = {
        timeOut: '10000',
        positionClass: 'toast-top-right'
      };

      // Check if MTTD is set below minimums and display error message
      if (isMttdSetTooLow) {
        set(this, 'customMttdClasses', `${customMttdClasses} te-input--error`);
        notifications.error(mttdErrMsg, 'MTTD range error', toastOptions);
        document.querySelector('#custom-tune-mttd').select();
        return;
      } else {
        notifications.clear();
        setProperties(this, {
          customMttdClasses: 'form-control te-input',
          isPerformanceDataLoading: true
        });
      }

      if (this.get('selectedTuneType') === 'custom') {
        // Trigger preview with custom params
        this.send('triggerTuningSequence', this.get('customTuneQueryString'));
      } else {
        // When user wants to preview using "current" settings, our request does not contain custom params.
        this.send('triggerTuningSequence', defaultConfig);
      }

      // Reset table filter
      set(this, 'filterBy', 'All');
    }
  }

});
