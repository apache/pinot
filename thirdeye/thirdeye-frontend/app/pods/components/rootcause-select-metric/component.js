import Component from '@ember/component';
import fetch from 'fetch';
import { toBaselineUrn, toCurrentUrn, filterPrefix, toMetricLabel } from 'thirdeye-frontend/utils/rca-utils';
import { autocompleteAPI } from 'thirdeye-frontend/utils/api/self-serve';
import { task, timeout } from 'ember-concurrency';
import _ from 'lodash';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import { computed, getProperties } from '@ember/object';
export default Component.extend({
  classNames: ['rootcause-select-metric-dimension'],

  selectedUrn: null, // ""

  onSelection: null, // function (metricUrn)

  onFocus: null,

  placeholder: 'Search for a Metric',

  //
  // internal
  //
  mostRecentSearch: null, // promise

  selectedUrnCache: null, // ""

  selectedMetric: null, // {}

  searchInputSelector: '#ember-basic-dropdown-wormhole input',

  /**
   * @summary Concurrency task that triggers returning the selected metrics and related metrics lists
   * @return {Array} array of groupName and options list
   * @example
     [
       { groupName: 'Selected Metrics', options: [{alias:'one', id: '1'}, {alias:'two', id: '2'}, {alias:'three', id: '3'}] }
     ]
   */
  recommendedMetrics: computed('entities', 'selectedUrns', function () {
    const { selectedUrns, entities } = this.getProperties('selectedUrns', 'entities');

    // NOTE: all of this is very hacky as it merges data from two different sources - entities and the autocomplete

    const selectedMetrics = filterPrefix(selectedUrns, 'thirdeye:metric:')
      .filter((urn) => urn in entities)
      .map((urn) => {
        const entity = entities[urn];
        const labelParts = entity.label.split('::');
        return {
          alias: entity.label,
          urn: entity.urn,
          name: toMetricLabel(urn, entities),
          dataset: labelParts[0],
          isSelected: true
        };
      });

    const relatedMetrics = filterPrefix(Object.keys(entities), 'thirdeye:metric:')
      .filter((urn) => urn in entities && !selectedUrns.has(urn))
      .map((urn) => {
        const entity = entities[urn];
        const labelParts = entity.label.split('::');
        return {
          alias: entity.label,
          urn: entity.urn,
          name: toMetricLabel(urn, entities),
          dataset: labelParts[0],
          isSelected: false
        };
      });

    return [
      { groupName: 'Selected Metrics', options: _.sortBy(selectedMetrics || [], (row) => row.alias) },
      { groupName: 'Related Metrics', options: _.sortBy(relatedMetrics || [], (row) => row.alias) }
    ];
  }),

  /**
   * Ember concurrency task that triggers the metric autocomplete
   */
  searchMetrics: task(function* (metric) {
    yield timeout(1000);
    return fetch(autocompleteAPI.metric(metric)).then(checkStatus);
  }),

  didReceiveAttrs() {
    this._super(...arguments);

    const { selectedUrn, selectedUrnCache } = this.getProperties('selectedUrn', 'selectedUrnCache');

    if (!_.isEqual(selectedUrn, selectedUrnCache)) {
      this.set('selectedUrnCache', selectedUrn);

      if (selectedUrn) {
        const url = `/data/metric/${selectedUrn.split(':')[2]}`;
        fetch(url)
          .then(checkStatus)
          .then((res) => {
            this.set('selectedMetric', res);
          });
      } else {
        this.set('selectedMetric', null);
      }
    }
  },

  actions: {
    /**
     * Action handler for metric recomendations on currently selected metrics
     * @param {Object} metric
     */
    recommendedMetricsAction(metric) {
      this.send('onChange', metric);
    },

    /**
     * Action handler for metric search changes
     * @param {Object} metric
     */
    onChange(metric) {
      const { onSelection } = this.getProperties('onSelection');
      if (!onSelection) {
        return;
      }

      const { urn, id } = metric;
      if (!urn && !id) {
        return;
      }

      const metricUrn = urn ? urn : `thirdeye:metric:${id}`;

      const updates = { [metricUrn]: true, [toBaselineUrn(metricUrn)]: true, [toCurrentUrn(metricUrn)]: true };

      onSelection(updates);
    },

    /**
     * Inserts the selected metric alias into the power-select search field for easy modification
     * @param {Object} selectObj - metric selected
     */
    onFocus(selectObj) {
      const { selectionEditable, searchInputSelector } = getProperties(
        this,
        'onFocus',
        'selectionEditable',
        'searchInputSelector'
      );
      if (selectionEditable && selectObj.isActive && selectObj.selected) {
        const selectInputEl = document.querySelector(searchInputSelector);
        selectInputEl.value = selectObj.selected.alias;
        selectInputEl.select();
      }
    },

    /**
     * Performs a search task while cancelling the previous one
     * @param {Array} metrics
     */
    onSearch(metrics) {
      const lastSearch = this.get('mostRecentSearch');
      if (lastSearch) {
        lastSearch.cancel();
      }

      const task = this.get('searchMetrics');
      const taskInstance = task.perform(metrics);
      this.set('mostRecentSearch', taskInstance);

      return taskInstance;
    }
  }
});
