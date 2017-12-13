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
import { toFilters, toFilterMap, filterPrefix, toCurrentUrn, toBaselineUrn, appendFilters } from 'thirdeye-frontend/helpers/utils';
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

export default Ember.Component.extend({
  context: null,

  onContext: null,

  onSelection: null,

  selectedMetricUrn: null,

  selectedFilters: null,

  filterOptions: {},

  /**
   * filter options
   * @type {Object}
   */
  filterOptionsObserver: Ember.observer('selectedMetricUrn', function () {
    const { selectedMetricUrn } = this.getProperties('selectedMetricUrn');

    if (!selectedMetricUrn) { return {}; }

    const id = selectedMetricUrn.split(':')[2];
    return fetch(`/data/autocomplete/filters/metric/${id}`)
      .then(res => res.json())
      .then(res => this.set('filterOptions', res));
  }),


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
    onMetricContext(updates) {
      const { onSelection, otherUrns } = this.getProperties('onSelection', 'otherUrns');

      const selected = filterPrefix(Object.keys(updates), 'thirdeye:metric:').filter(urn => updates[urn]);

      const nonMetricUrns = [...otherUrns].filter(urn => !urn.startsWith('thirdeye:metric:'));
      const newOtherUrns = [...nonMetricUrns, ...selected];

      this.set('otherUrns', newOtherUrns);
      this.send('updateContext');
    },

    /**
     * Adds current primary metric selection to chart (including filters)
     * @returns {undefined}
     */
    onMetricChart() {
      const { onSelection, context } = this.getProperties('onSelection', 'context');

      const filterUrns = filterPrefix(context.urns, 'thirdeye:dimension:');
      const metricUrns = filterPrefix(context.urns, 'thirdeye:metric:');

      const appendedUrns = metricUrns.map(urn => appendFilters(urn, toFilters(filterUrns)));

      const currentUrns = appendedUrns.map(toCurrentUrn);
      const baselineUrns = appendedUrns.map(toBaselineUrn);
      const allUrns = new Set([...appendedUrns, ...currentUrns, ...baselineUrns]);

      const updates = [...allUrns].reduce((agg, urn) => {
        agg[urn] = true;
        return agg;
      }, {});

      onSelection(updates);
    }
  }
});
