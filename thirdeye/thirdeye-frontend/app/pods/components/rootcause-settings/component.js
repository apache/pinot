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
import fetch from 'fetch';
import { toFilters, toFilterMap, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import _ from 'lodash'

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
  onSelection: null,
  urnString: null,
  analysisRangeStart: null,
  analysisRangeEnd: null,


  /**
   * Formatted max date
   * @return {String}
   */
  datePickerMaxDate: Ember.computed({
    get() {
      return moment().endOf('day');
    }
  }),

  /**
   * Formatted anomaly start date
   * @return {String}
   */
  datePickerAnomalyRangeStart: Ember.computed('anomalyRangeStart', {
    get() {
      const start = this.get('anomalyRangeStart');

      return moment(start).format(serverDateFormat);
    }
  }),

  /**
   * Formatted anomaly end date
   * @return {String}
   */
  datePickerAnomalyRangeEnd: Ember.computed('anomalyRangeEnd', {
    get() {
      const end = this.get('anomalyRangeEnd');

      return moment(end).format(serverDateFormat);
    }
  }),

  /**
   * filter options
   * @type {Object}
   */
  filterOptions: {},

  /**
   * Selected filters
   * @type {String} - a JSON string
   */
  filters: JSON.stringify({}),

  /**
   * Selected primary metric(s)
   * @type {Number}
   */
  primaryMetricUrn: Ember.computed('otherUrns', function () {
    const otherUrns = this.get('otherUrns');
    const metricUrns = filterPrefix(otherUrns, 'thirdeye:metric:');

    if (_.isEmpty(metricUrns)) { return null; }

    return metricUrns[0];
  }),

  /**
   * filter options
   * @type {Object}
   */
  filterOptionsObserver: Ember.observer('otherUrns', function () {
    // TODO anti pattern - refactor into separate component?

    const otherUrns = this.get('otherUrns');
    const metricUrns = filterPrefix(otherUrns, 'thirdeye:metric:');

    if (_.isEmpty(metricUrns)) { return {}; }

    const id = metricUrns[0].split(':')[2];
    return fetch(`/data/autocomplete/filters/metric/${id}`)
      .then(res => res.json())
      .then(res => this.set('filterOptions', res));
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
      filters
    });
  },

  didReceiveAttrs() {
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
        filters,
        otherUrns,
        context
      } = this.getProperties(
        'otherUrns',
        'filters',
        'anomalyRangeStart',
        'anomalyRangeEnd',
        'context');
      const onChange = this.get('onChange');

      const filterUrns = _filterToUrn(filters);

      const newContext = Object.assign({}, context, {
        urns: new Set([...otherUrns, ...filterUrns]),
        anomalyRange: [moment(anomalyRangeStart).valueOf(), moment(anomalyRangeEnd).valueOf()]
      });

      onChange(newContext);
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
     * Updates the filters
     * @method onFiltersChange
     * @param {Object} filters currently selected filters
     * @return {undefined}
     */
    onFiltersChange(filters) {
      this.set('filters', filters);
      this.send('updateContext');
    },

    /**
     * Updates the primary metric(s)
     * @method onMetricSelection
     * @param {Object} updates metric selection/deselection
     * @return {undefined}
     */
    onMetricSelection(updates) {
      const { onSelection, otherUrns } = this.getProperties('onSelection', 'otherUrns');

      if (onSelection) {
        onSelection(updates);
      }

      const selected = filterPrefix(Object.keys(updates), 'thirdeye:metric:').filter(urn => updates[urn]);

      const nonMetricUrns = [...otherUrns].filter(urn => !urn.startsWith('thirdeye:metric:'));
      const newOtherUrns = [...nonMetricUrns, ...selected];

      this.set('otherUrns', newOtherUrns);
      this.send('updateContext');
    }
  }
});
