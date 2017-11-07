/**
 * Rootcause settings component. It contains the logic needed for displaying
 * the rca settings box
 * @module components/rootcause-settings
 * @property {Boolean} isEditingIterationSegment - Indicates if user is editing
 *                                                 an iteration
 * @property {Boolean} isDisableInput  - Indicates if segment inputs should
 *                                       be disabled
 * @property {Object}  selectorSpec    - An object containing selectorSpec info
 * @property {Number}  segmentIndex    - Segment position in array
 * @property {Array}   segmentsList    - "Target" dropdown select options
 * @property {String}  segmentType     - Segment type
 * @property {Object}  test            - An object containing test info
 * @property {Array}   customSelectors - An array of objects containing custom
 *                                       selectors info
 * @property {Object}  onDeleteSegment - Closure action to bubble up to parent
 *                                       when a segment is deleted
 * @property {Object}  onMoveSegment   - Closure action to bubble up to parent
 *                                       when a segment index is changed
 * @example
 * {{test/iterations/setup/segments/iteration-segment
 *   isEditingIterationSegment=isEditingIterationSegment
 *   isDisableInput=isDisableInput
 *   selectorSpec=selectorSpec
 *   segmentIndex=segmentIndex
 *   segmentsList=segmentsList
 *   segmentType=segment.type
 *   test=test
 *   customSelectors=customSelectors
 *   onDeleteSegment=(action "onDeleteSegment")
 *   onMoveSegment=(action "onMoveSegment")}}
 * @exports rootcause-settings
 * @author apucher, yyuen
 */

import Ember from 'ember';
import moment from 'moment';

export default Ember.Component.extend({
  context: null, // { urns, anomalyRange, baselineRange, analaysisRange }

  onChange: null, // function (context)

  urnString: null,

  anomalyRangeStart: null,

  anomalyRangeEnd: null,

  baselineRangeStart: null,

  baselineRangeEnd: null,

  analysisRangeStart: null,

  analysisRangeEnd: null,

  // datepicker range
  datePickerAnomalyRangeStart: Ember.computed.reads('anomalyRangeStart'),
  datePickerAnomalyRangeEnd: Ember.computed.reads('anomalyRangeEnd'),
  datePickerAnalysisRangeStart: Ember.computed.reads('analysisRangeStart'),
  datePickerAnalysisRangeEnd: Ember.computed.reads('analysisRangeEnd'),

  // selected Granularity
  granularity: null,

  // granularities: Ember.computed.reads('model.granularities'),
  granularities: ['MINUTES', 'HOURS', 'DAYS'],

  // selected Compare Mode
  compareMode: 'WoW',
  // compare mode options
  compareModeOptions: ['WoW', 'Wo2W', 'Wo3W', 'Wo4W'],

  // predefined ranges for display window
  predefinedAnalysisRanges: {
    'Last 2 days': [
      moment().subtract(3, 'days').startOf('day'), 
      moment().subtract(1, 'days').endOf('day')
    ],
    'last 7 Days': [
      moment().subtract(7, 'days').startOf('day'),
      moment().subtract(1, 'days').endOf('day')
    ]
  },

  // metricFilters: Ember.computed.reads('model.metricFilters'),
  metricFilters: {},
  filters: JSON.stringify({}),

  /**
   * Indicates the date format to be used based on granularity
   * @type {String}
   */
  uiDateFormat: Ember.computed('granularity', function () {
    const granularity = this.get('granularity');

    switch (granularity) {
      case 'DAYS':
        return 'MMM D, YYYY';
      case 'HOURS':
        return 'MMM D, YYYY h a';
      default:
        return 'MMM D, YYYY hh:mm a';
    }
  }),

  /**
   * Indicates the allowed date range picker increment based on granularity
   * @type {Number}
   */
  timePickerIncrement: Ember.computed('granularity', function () {
    const granularity = this.get('granularity');

    switch (granularity) {
      case 'DAYS':
        return 1440;
      case 'HOURS':
        return 60;
      default:
        return 5;
    }
  }),

  _updateFromContext() {
    const { urns, anomalyRange, baselineRange, analysisRange } = this.get('context');
    this.setProperties({ urnString: [...urns].join(','),
      anomalyRangeStart: anomalyRange[0], anomalyRangeEnd: anomalyRange[1],
      baselineRangeStart: baselineRange[0], baselineRangeEnd: baselineRange[1],
      analysisRangeStart: analysisRange[0], analysisRangeEnd: analysisRange[1]
    });
  },

  didUpdateAttrs() {
    this._super(...arguments);
    this._updateFromContext();
  },

  didInsertElement() {
    this._super(...arguments);
    this._updateFromContext();
  },

  actions: {
    updateContext() {
      const { urnString, anomalyRangeStart, anomalyRangeEnd, baselineRangeStart, baselineRangeEnd, analysisRangeStart, analysisRangeEnd } =
        this.getProperties('urnString', 'anomalyRangeStart', 'anomalyRangeEnd', 'baselineRangeStart', 'baselineRangeEnd', 'analysisRangeStart', 'analysisRangeEnd');
      const onChange = this.get('onChange');
      if (onChange != null) {
        const urns = new Set(urnString.split(','));
        const anomalyRange = [parseInt(anomalyRangeStart), parseInt(anomalyRangeEnd)];
        const baselineRange = [parseInt(baselineRangeStart), parseInt(baselineRangeEnd)];
        const analysisRange = [parseInt(analysisRangeStart), parseInt(analysisRangeEnd)];
        const newContext = { urns, anomalyRange, baselineRange, analysisRange };
        onChange(newContext);
      }
    },
    // date picker actions
    setAnomalyDateRange(start, end) {
      const anomalyRangeStart = moment(start).valueOf();
      const anomalyRangeEnd = moment(end).valueOf();

      this.setProperties({
        anomalyRangeStart,
        anomalyRangeEnd
      });
    },
    // date picker actions
    setDisplayDateRange(start, end) {
      const analysisRangeStart = moment(start).valueOf();
      const analysisRangeEnd = moment(end).valueOf();

      this.setProperties({
        analysisRangeStart,
        analysisRangeEnd
      });
    },

    onModeChange(compareMode) {
      const {
        analysisRangeStart,
        analysisRangeEnd
      } = this.getProperties('analysisRangeStart', 'analysisRangeEnd');
      const offset = {
        WoW: 1,
        Wo2W: 2,
        Wo3W: 3,
        Wo4W: 4
      }[compareMode];

      const baselineRangeStart = moment(analysisRangeStart).subtract(offset, 'days').valueOf();
      const baselineRangeEnd = moment(analysisRangeEnd).subtract(offset, 'days').valueOf();

      this.setProperties({
        compareMode,
        baselineRangeStart,
        baselineRangeEnd
      });
    },

    onGranularityChange() {
      debugger;
    },
    
    hideDatePicker() {
      // alert('hiding date picker');
    },
    cancelDatePicker() {
      // alert('cancelDatePicker');
    }
  }
});
