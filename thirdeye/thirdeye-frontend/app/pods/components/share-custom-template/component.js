/**
 * Filter Bar Component
 * Constructs a filter bar based on a config file
 * @module components/filter-bar
 * @property {object[]} config            - [required] array of objects (config file) passed in from the route that sets
 *                                          up the filter bar sub-filters
 * @property {object} entities            - [required] data used to filter
 * @property {array}  onFilterSelection   - [required] closure action to bubble to controller on filter selection
 *
 * @example
 * {{filter-bar
 *   config=filterBarConfig
 *   entities=entities
 *   onFilter=(action "onFilterSelection")}}
 *
 * @exports filter-bar
 */
import Component from '@ember/component';
import { get, set, computed, getProperties } from '@ember/object';
import _ from 'lodash';
import { task } from 'ember-concurrency';
import { humanizeFloat, checkStatus } from 'thirdeye-frontend/utils/utils';
import moment from 'moment';
import floatToPercent from 'thirdeye-frontend/utils/float-to-percent';
import Ember from 'ember';
import config from 'thirdeye-frontend/config/environment';

/* eslint-disable ember/avoid-leaking-state-in-ember-objects */
export default Component.extend({
  classNames: ['share-custom-template'],
  /**
   * @summary The actaul visual cell data mapping to render into the custom header table.
   */
  customHeaderMapping: [],

  /**
   * Triggered when changes are made to attributes by components outside of filter bar
   * (i.e. changes in search form), filter results based on newly updated filteredUrns
   */
  async didReceiveAttrs() {
    set(this, 'customHeaderMapping', []);
    //TODO: not sure we want to do this in the
    const customHeaderMapping = await get(this, '_getConfigTemplateOffsetsMapping').perform();
    set(this, 'customHeaderMapping', _.cloneDeep(customHeaderMapping));
    set(this, 'startDateDisplay', moment(get(this, 'start')).format('MM/DD/YYYY'));
  },

  /**
   * Returns object that represents mapping between attributes in events and input values in config
   * @type {Object}
   */
  customHeaderMappingComplete: computed('customHeaderMapping', function () {
    return get(this, 'customHeaderMapping');
  }),

  /**
   * @summary Retrieve config for custome template header by app name and fetch the metric data
   * @return {Array}
   TODO: - lohuynh (config)
   */
  _getConfigTemplateOffsetsMapping: task(function* () {
    const config = yield get(this, 'config');
    const customDataMapping = yield get(this, '_createCustomHeaderMapping').perform(config.entities);
    return customDataMapping;
  }).drop(),

  _createCustomHeaderMapping: task(function* (entities) {
    // Avoid ember-testing-container inconsistencies. We want to returned the config in test vs fetching live data.
    if (Ember.testing) {
      return entities;
    }

    if (!entities) {
      return;
    }

    let index = 0;
    const customHeaderMapping = get(this, 'customHeaderMapping');
    const { start, end } = getProperties(this, 'start', 'end');
    const timeZone = config.timeZone;

    for (const entity of entities) {
      if (!customHeaderMapping[index]) {
        customHeaderMapping[index] = [];
      }

      for (const cell of entity) {
        cell.index = index;
        if (cell.type && cell.type === 'label') {
          customHeaderMapping[index].push(cell);
        } else {
          const metrics = cell.metrics;
          const offsetsStr = cell.offsets.join(',');
          const sumOffsets = [];
          if (metrics) {
            for (const metric of metrics) {
              const offsets = yield fetch(
                `/rootcause/metric/aggregate/batch?urn=${metric}&start=${start}&end=${end}&offsets=${offsetsStr}&timezone=${timeZone}`
              )
                .then(checkStatus)
                .then((res) => res);
              sumOffsets.push(offsets);
            }
            // assumes maximum of two metrics and compares the two
            let reducedSumOffsets;
            if (Array.isArray(sumOffsets[0]) && sumOffsets.length > 1 && Array.isArray(sumOffsets[1])) {
              reducedSumOffsets = [sumOffsets[0][0], sumOffsets[1][0]];
            } else {
              reducedSumOffsets = sumOffsets.reduce((arg, current) => {
                return [arg[0] + current[0], arg[1] + current[1]];
              });
            }
            cell.summary = [
              humanizeFloat(reducedSumOffsets[0]),
              floatToPercent(reducedSumOffsets[0] / reducedSumOffsets[1] - 1)
            ];
            customHeaderMapping[index].push(cell);
          }
        }
      }
      index++;
    }

    return customHeaderMapping;
  }).drop(),

  actions: {
    /**
     * Triggered when user selects an event type (down arrow in filter bar)
     * @param {String} eventType
     */
    selectEventType(/* eventType */) {}
  }
});
