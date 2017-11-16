/**
 * Rootcause settings component. It contains the logic needed for displaying
 * the rca settings box
 * @module components/rootcause-settings
 * @property {Object} config          - contains dropdown and filters values
 * @property {Object} context         - contains user selected options from query params
 * @property {Object} onChange        - Closure action to bubble up to parent
 *                                      when the settings change
 * @example
  {{rootcause-settings
    config=config
    context=context
    onChange=(action "settingsOnChange")
  }}
 * @exports rootcause-settings
 * @author apucher, yyuen
 */

import Ember from 'ember';
import moment from 'moment';
import { toFilters, toFilterMap } from '../../../helpers/utils';

// TODO: move this to a utils file (DRYER)
const _filterToUrn = (filters) => {
  const urns = [];
  const filterObject = JSON.parse(filters);
  Object.keys(filterObject)
    .forEach((key) => {
      const filterUrns = filterObject[key]
        .map(dimension => `thirdeye:dimension:${key}:${dimension}:provided`);
      urns.push(...filterUrns);
    });

  return urns;
};

/**
 * Date formate the date picker component expects
 * @type String
 *
 */
const serverDateFormat = 'YYYY-MM-DD HH:mm';

export default Ember.Component.extend({
  onChange: null,
  urnString: null,
  anomalyRangeStart: null,
  anomalyRangeEnd: null,
  analysisRangeStart: null,
  analysisRangeEnd: null,


  /**
   * Formatted anomaly start date
   * @return {String}
   */
  datePickerAnomalyRangeStart: Ember.computed('anomalyRangeStart', {
    get() {
      const start = this.get('anomalyRangeStart');

      return start ? moment(+start).format(serverDateFormat) : moment().format(serverDateFormat);
    }
  }),

  /**
   * Formatted anomaly end date
   * @return {String}
   */
  datePickerAnomalyRangeEnd: Ember.computed('anomalyRangeEnd', {
    get() {
      const end = this.get('anomalyRangeEnd');

      return end ? moment(+end).format(serverDateFormat) : moment().format(serverDateFormat);
    }
  }),

  /**
   * Formatted analysis start date
   * @return {String}
   */
  datePickerAnalysisRangeStart: Ember.computed('analysisRangeStart', {
    get() {
      const start = this.get('analysisRangeStart');

      return start ? moment(+start).format(serverDateFormat) : moment().format(serverDateFormat);
    }
  }),

  /**
   * Formatted analysis end date
   * @return {String}
   */
  datePickerAnalysisRangeEnd: Ember.computed('analysisRangeEnd', {
    get() {
      const end = this.get('analysisRangeEnd');

      return end ? moment(+end).format(serverDateFormat) : moment().format(serverDateFormat);
    }
  }),

  /**
   * Selected Granularity
   * @type {String}
   */
  granularity: null,

  /**
   * Granularities Options
   * @type {String[]}
   */
  granularityOptions: Ember.computed.reads('config.granularityOptions'),
  /**
   * Compare Mode Options
   * @type {String[]}
   */
  compareModeOptions: Ember.computed.reads('config.compareModeOptions'),

  /**
   * filter options
   * @type {Object}
   */
  filterOptions: Ember.computed.reads('config.filterOptions'),

  /**
   * Selected Compare Mode
   * @type {String}
   */
  compareMode: 'WoW',

  /**
   * Predefined Custom Ranges for
   * the display region
   * @type {Object}
   */
  predefinedAnalysisRanges: {
    'Last 2 days': [
      moment().subtract(3, 'days').startOf('day'),
      moment().subtract(1, 'days').endOf('day')
    ],
    'Last 7 days': [
      moment().subtract(7, 'days').startOf('day'),
      moment().subtract(1, 'days').endOf('day')
    ]
  },

  /**
   * Selected filters
   * @type {String} - a JSON string
   */
  filters: JSON.stringify({}),

  /**
   * Indicates the unit of the granularity
   * @type {String}
   */
  granularityUnit: Ember.computed('granularity', function () {
    const granularity = this.get('granularity');
    const units = ['MINUTES', 'HOURS', 'DAYS'];

    for (let i = 0; i < units.length; i++) {
      if (granularity.endsWith(units[i])) {
        return units[i];
      }
    }

    return 'MINUTES';
  }),

  /**
   * Indicates the date format to be used based on granularityUnit
   * @type {String}
   */
  uiDateFormat: Ember.computed('granularityUnit', function () {
    const granularityUnit = this.get('granularityUnit');

    switch (granularityUnit) {
      case 'DAYS':
        return 'MMM D, YYYY';
      case 'HOURS':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  /**
   * Indicates the allowed date range picker increment based on granularityUnit
   * @type {Number}
   */
  timePickerIncrement: Ember.computed('granularityUnit', function () {
    const granularityUnit = this.get('granularityUnit');

    switch (granularityUnit) {
      case 'DAYS':
        return 1440;
      case 'HOURS':
        return 60;
      default:
        return 5;
    }
  }),


  /**
   * Parses the context and sets component's props
   */
  _updateFromContext() {
    const {
      urns,
      anomalyRange,
      analysisRange,
      granularity,
      compareMode
    } = this.get('context');

    const filterUrns = Array.from(urns).filter(urn => urn.startsWith('thirdeye:dimension:'));
    const otherUrns = Array.from(urns).filter(urn => !urn.startsWith('thirdeye:dimension:'));
    const filters = JSON.stringify(toFilterMap(toFilters(filterUrns)));

    this.setProperties({
      otherUrns,
      anomalyRangeStart: anomalyRange[0], anomalyRangeEnd: anomalyRange[1],
      analysisRangeStart: analysisRange[0], analysisRangeEnd: analysisRange[1],
      granularity,
      compareMode,
      filters
    });
  },

  init() {
    this._super(...arguments);

    this._updateFromContext();
  },

  didReceiveAttrs() {
    this._super(...arguments);

    this._updateFromContext();
  },

  didInsertElement() {
    this._super(...arguments);

    this._updateFromContext();
  },

  actions: {
    /**
     * Grabs Properties and sends them to the parent via an action
     * @method updateContext
     * @return {undefined}
     */
    updateContext() {
      const {
        anomalyRangeStart,
        anomalyRangeEnd,
        analysisRangeStart,
        analysisRangeEnd,
        granularity,
        filters,
        compareMode,
        otherUrns
      } = this.getProperties(
        'otherUrns',
        'granularity',
        'filters',
        'anomalyRangeStart',
        'anomalyRangeEnd',
        'compareMode',
        'analysisRangeStart',
        'analysisRangeEnd');
      const onChange = this.get('onChange');


      if (onChange != null) {
        const filterUrns = _filterToUrn(filters);
        const urns = new Set([...otherUrns, ...filterUrns]);
        const anomalyRange = [parseInt(anomalyRangeStart), parseInt(anomalyRangeEnd)];
        const analysisRange = [parseInt(analysisRangeStart), parseInt(analysisRangeEnd)];
        const newContext = { urns, anomalyRange, analysisRange, granularity, compareMode };
        onChange(newContext);
      }
    },

    /**
     * Sets the new anomaly region date in ms
     * @method setAnomalyDateRange
     * @param {String} start  - stringified start date
     * @param {String} end    - stringified end date
     * @return {undefined}
     */
    setAnomalyDateRange(start, end) {
      const anomalyRangeStart = moment(start).valueOf();
      const anomalyRangeEnd = moment(end).valueOf();

      this.setProperties({
        anomalyRangeStart,
        anomalyRangeEnd
      });
      this.send('updateContext');
    },

    /**
     * Sets the new display date in ms
     * @method setDisplayDateRange
     * @param {String} start  - stringified start date
     * @param {String} end    - stringified end date
     * @return {undefined}
     */
    setDisplayDateRange(start, end) {
      const analysisRangeStart = moment(start).valueOf();
      const analysisRangeEnd = moment(end).valueOf();

      this.setProperties({
        analysisRangeStart,
        analysisRangeEnd
      });
      this.send('updateContext');
    },


    /**
     * Updates the compare mode
     * @method onModeChange
     * @param {String} compareMode baseline compare mode
     * @return {undefined}
     */
    onModeChange(compareMode) {
      this.set('compareMode', compareMode);
      this.send('updateContext');
    },

    /**
     * Updates the granularity
     * @method onGranularityChange
     * @param {String} granularity the selected granularity
     * @return {undefined}
     */
    onGranularityChange(granularity) {
      this.set('granularity', granularity);
      this.send('updateContext');
    },

    /**
     * Updates the filters
     * @method onFiltersChange
     * @param {Object} filters currently selected filters
     * @return {undefined}
     */
    onFiltersChange(filters) {
      this.set('filters', filters);
      this.send('updateContext');
    }
  }
});
