import Helper from '@ember/component/helper';
import { htmlSafe } from '@ember/string';
import {
  filterPrefix,
  toBaselineUrn,
  toCurrentUrn,
  toMetricLabel,
  toEventLabel,
  toColorDirection,
  isInverse
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import moment from 'moment';
import d3 from 'd3';

/**
 * Massages the hovered urns
 * @param {Array} urns - list of hovered urns
 */
const getUrns = (urns) => {
  const metricUrns = filterPrefix(urns, 'thirdeye:metric:');
  const eventUrns = filterPrefix(urns, 'thirdeye:event:');
  return [metricUrns, eventUrns];
};

/**
 * Return the labels for the hovered urns
 */
const getLabel = (entities, hoverUrns) => {
  const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');
  const eventUrns = filterPrefix(hoverUrns, 'thirdeye:event:');
  const labels = {};
  metricUrns.forEach(urn => labels[urn] = toMetricLabel(urn, entities));
  eventUrns.forEach(urn => labels[urn] = toEventLabel(urn, entities));

  return labels;
};

/**
 * Returns an Object mapping the urns to the timeseries
 * @param {Object} timeseries - all time series
 * @param {Array} hoverUrns   - list of hovered urns
 */
const getTimeseriesLookup = (timeseries, hoverUrns) => {
  const frontendUrns = filterPrefix(hoverUrns, 'frontend:metric:');
  const lookup = {};
  frontendUrns.forEach(urn => {
    const ts = timeseries[urn];
    lookup[urn] = ts.timestamps.map((t, i) => [parseInt(t, 10), ts.values[i]]);
  });

  return lookup;
};

/**
 * Returns an Mapping of urns to timeseries values
 * @param {Array} hoverUrns       - list of hovered urns
 * @param {Number} hoverTimestamp - hovered time in unix ms
 * @param {Object} lookup         - the mapping object
 */
const getValues = (hoverUrns, hoverTimestamp, lookup, entities) => {
  const metricUrns = filterPrefix(hoverUrns, 'thirdeye:metric:');

  hoverTimestamp = parseInt(hoverTimestamp, 10);

  const values = {};
  metricUrns.forEach(urn => {
    // find first smaller or equal element
    const currentLookup = (lookup[toCurrentUrn(urn)] || []).reverse();
    const currentTimeseries = currentLookup.find(t => t[0] <= hoverTimestamp && t[1] != null);
    const current = currentTimeseries ? currentTimeseries[1] : parseFloat('NaN');

    const baselineLookup = (lookup[toBaselineUrn(urn)] || []).reverse();
    const baselineTimeseries = baselineLookup.find(t => t[0] <= hoverTimestamp && t[1] != null);
    const baseline = baselineTimeseries ? baselineTimeseries[1] : parseFloat('NaN');

    const change = current / baseline - 1;

    values[urn] = {
      current: d3.format('.3s')(humanizeFloat(current)),
      baseline: d3.format('.3s')(humanizeFloat(baseline)),
      delta: humanizeChange(change),
      color: toColorDirection(change, isInverse(urn, entities))
    };
  });

  return values;
};

/**
 * Return an Mapping of urns to colors
 */
const getColors = (entities, hoverUrns) => {
  return filterPrefix(hoverUrns, ['thirdeye:metric:', 'thirdeye:event:'])
    .filter(urn => entities[urn])
    .reduce((agg, urn) => {
      agg[urn] = entities[urn].color;
      return agg;
    }, {});
};

/**
 * returns an html template for the tooltip
 */
export default Helper.extend({
  compute(hash) {
    const {
      entities,
      timeseries,
      hoverUrns,
      hoverTimestamp
    } = hash;

    const [ metricUrns, eventUrns ] = getUrns(hoverUrns);
    const humanTimeStamp = moment(hoverTimestamp).format('MMM DD, hh:mm a');

    // TODO cache these things for performance
    const labels = getLabel(entities, hoverUrns);
    const lookup = getTimeseriesLookup(timeseries, hoverUrns);
    const values = getValues(hoverUrns, hoverTimestamp, lookup, entities);
    const colors = getColors(entities, hoverUrns);

    /** TODO: abstract the js out of the template */
    return htmlSafe(`
      <div class="te-tooltip">
        <h5 class="te-tooltip__header">${humanTimeStamp} (PDT)</h5>
        <div class="te-tooltip__body">
          ${metricUrns.map((urn) => {
            return `
              <div class="te-tooltip__item">
                <div class="te-tooltip__indicator">
                  <span class="entity-indicator entity-indicator--flat entity-indicator--${colors[urn]}"></span>
                </div>
                <span class="te-tooltip__label">${labels[urn]}</span>
                <span class="te-tooltip__value te-tooltip__value--${values[urn].color}">
                  ${values[urn].delta}
                </span>
              </div>
              <div class="te-tooltip__item--indent te-tooltip__item--small">
                <span>Current/Baseline: </span>
                <span class="te-tooltip__value">${values[urn].current} / ${values[urn].baseline}</span>
              </div>
            `;
          }).join('')}
          <div class="te-tooltip__events ${(!eventUrns.length) ? 'te-tooltip__events--hidden' : ''}">
            ${eventUrns.map((urn) => {
              return `
                <div class="te-tooltip__item">
                  <div>
                    <span class="entity-indicator entity-indicator--${colors[urn]}"></span>
                  </div>
                  <span class="te-tooltip__label">${labels[urn]}</span>
                </div>
              `;
            }).join('')}
          </div>
        </div>
      </div>
    `);
  }
});
