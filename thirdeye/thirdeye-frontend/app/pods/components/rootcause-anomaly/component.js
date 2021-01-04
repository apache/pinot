import Component from '@ember/component';
import { computed, setProperties, getProperties, get, set } from '@ember/object';
import moment from 'moment';
import {
  filterPrefix,
  toOffsetUrn,
  toColorDirection,
  isInverse,
  toMetricLabel,
  makeTime,
  dateFormatFull
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import { equal, reads } from '@ember/object/computed';
import { anomalyResponseMap } from 'thirdeye-frontend/utils/anomaly';

const ROOTCAUSE_HIDDEN_DEFAULT = 'default';

const OFFSETS = ['current', 'predicted', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];

/**
 * Maps the status from the db to something human readable to display on the form
 * @type {Object}
 */
const ANOMALY_OPTIONS_MAPPING = anomalyResponseMap;

export default Component.extend({
  classNames: ['rootcause-anomaly'],

  entities: null, // {}

  /**
   * Metrics aggregated over a search context-specific interval keyed by metric urn
   * @type {Object} - key is metric urn, and value is a number calculated from an aggregated set of numbers interpreted
   * based on a selected context (i.e. analysis period, baseline)
   * @example
   * {
   *  metric:offset:id: 12345,
   *  metric:offset:id2: 12345,
   *  ...
   * }
   */
  aggregates: null,

  anomalyUrns: null, // Set

  onFeedback: null, // func (urn, feedback, comment)

  isHiddenUser: ROOTCAUSE_HIDDEN_DEFAULT,

  offsets: OFFSETS,

  /**
   * Array of human readable anomaly options for users to select
   * @type {string}
   */
  options: Object.keys(ANOMALY_OPTIONS_MAPPING),

  /**
   * Can be set by didReceiveAttrs if data inconsistent
   * @type {boolean}
   */
  warningValue: false,

  /**
   * A mapping of the status and a more human readable version
   * @type {Object}
   */
  optionsMapping: ANOMALY_OPTIONS_MAPPING,

  /**
   * Checks if anomalyRange from context is different than anomaly start and end
   * times
   * @type {boolean}
   */
  isRangeChanged: computed('anomalyRange', 'anomaly', function () {
    const anomalyRange = get(this, 'anomalyRange');
    const start = get(this, 'anomaly').start;
    const end = get(this, 'anomaly').end;
    return !(anomalyRange[0] === start && anomalyRange[1] === end);
  }),

  /**
   * Urn of an anomaly
   * @type {String}
   */
  anomalyUrn: computed('anomalyUrns', function () {
    const anomalyUrns = get(this, 'anomalyUrns');
    const anomalyEventUrn = filterPrefix(anomalyUrns, 'thirdeye:event:anomaly:');

    if (!anomalyEventUrn) return;

    return anomalyEventUrn[0];
  }),

  /**
   * Metric urn for anomaly topic metric
   * @type {string}
   */
  metricUrn: computed('anomalyUrns', function () {
    const { anomalyUrns } = getProperties(this, 'anomalyUrns');

    const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

    if (!metricUrns) {
      return;
    }

    return metricUrns[0];
  }),

  /**
   * Information about an anomaly displayed in the overview regarding its id, metric, dimensions, alert name, and duration
   * @type {Object}
   */
  anomaly: computed('entities', 'anomalyUrn', function () {
    const { entities, anomalyUrn } = getProperties(this, 'entities', 'anomalyUrn');

    if (!anomalyUrn || !entities || !entities[anomalyUrn]) return;

    return entities[anomalyUrn];
  }),

  /**
   * Anomaly function (alert) name
   * @type {string}
   */
  functionName: reads('anomaly.attributes.function.firstObject'),

  /**
   * Anomaly detection (alert) id
   * @type {string}
   */
  detectionConfigId: reads('anomaly.attributes.detectionConfigId.firstObject'),

  /**
   * Anomaly metric name from anomaly attributes
   * @type {string}
   */
  metric: reads('anomaly.attributes.metric.firstObject'),

  /**
   * Anomaly metric dataset name
   * @type {string}
   */
  dataset: reads('anomaly.attributes.dataset.firstObject'),

  /**
   * Anomaly feedback status
   * @type {string}
   */
  status: reads('anomaly.attributes.status.firstObject'),

  /**
   * Anomaly baseline as computed by anomaly function
   * @type {float}
   */
  predicted: reads('anomaly.attributes.baseline.firstObject'),

  /**
   * Anomaly current as computed by anomaly function
   * @type {float}
   */
  current: reads('anomaly.attributes.current.firstObject'),

  /**
   * Anomaly aggregate multiplier
   * @type {float}
   */
  aggregateMultiplier: reads('anomaly.attributes.aggregateMultiplier.firstObject'),

  /**
   * Anomaly granularity
   * @type {string}
   */
  metricGranularity: reads('anomaly.attributes.metricGranularity.firstObject'),

  /**
   * Anomaly unique identifier
   * @type {string}
   */
  anomalyId: computed('anomaly', function () {
    return get(this, 'anomaly').urn.split(':')[3];
  }),

  /**
   * Anomaly issue type for grouped anomaly (not supported yet)
   * @type {string}
   */
  issueType: null, // TODO

  /**
   * Formatted metric label of anomaly topic metric
   * @type {string}
   */
  metricLabel: computed('anomalyUrns', 'entities', function () {
    const { anomalyUrns, entities } = getProperties(this, 'anomalyUrns', 'entities');

    const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

    if (!metricUrns) {
      return;
    }

    const metricUrn = metricUrns[0];

    return toMetricLabel(metricUrn, entities);
  }),

  /**
   * Formatted anomaly start time
   * @type {string}
   */
  startFormatted: computed('anomaly', function () {
    return makeTime(get(this, 'anomaly').start).format(dateFormatFull);
  }),

  /**
   * Formatted anomaly end time
   * @type {string}
   */
  endFormatted: computed('anomaly', function () {
    return makeTime(get(this, 'anomaly').end).format(dateFormatFull);
  }),

  /**
   * Formatted anomaly duration
   * @type {string}
   */
  durationFormatted: computed('anomaly', function () {
    const anomaly = get(this, 'anomaly');
    return moment.duration(anomaly.end - anomaly.start).humanize();
  }),

  /**
   * Default state for hide/expand panel
   * @type {boolean}
   */
  requiresFeedback: equal('status', 'NO_FEEDBACK'),

  /**
   * Toggle for hide/expand panel
   * @type {boolean}
   */
  isHidden: computed('requiresFeedback', 'isHiddenUser', function () {
    const { requiresFeedback, isHiddenUser } = getProperties(this, 'requiresFeedback', 'isHiddenUser');
    if (isHiddenUser === ROOTCAUSE_HIDDEN_DEFAULT) {
      return !requiresFeedback;
    }
    return isHiddenUser;
  }),

  /**
   * External links for current anomaly
   * @type {Object} - an object with key as the link type and value as the url
   */
  anomalyLinks: computed('anomaly', function () {
    const { externalUrls = [] } = get(this, 'anomaly.attributes');
    let urls = {};

    if (externalUrls.length) {
      externalUrls.forEach((url) => {
        urls[url] = get(this, 'anomaly.attributes')[url][0]; // there will always be only 1 element in this array
      });
    }
    return urls;
  }),

  /**
   * Information about an anomaly's baselines, changes, and values to be displayed in the anomaly overview
   * @type {Object}
   * @example
   * {
   *   current: {
   *    change: 1.1,
   *    changeFormatted: +1.1,
   *    value: 5000
   *   }, {
   *   baseline: {
   *    change: 1.1,
   *    changeFormatted: 1.1,
   *    value: 1000
   *   }
   * }
   */
  anomalyInfo: computed('aggregates', 'anomalyUrns', 'entities', 'baseline', function () {
    const { metricUrn, entities } = getProperties(this, 'metricUrn', 'entities');

    const curr = this._getAggregate('current');

    const anomalyInfo = {};
    [...this.offsets].forEach((offset) => {
      const value = this._getAggregate(offset);
      const change = curr / value - 1;

      if (!Number.isNaN(value)) {
        anomalyInfo[offset] = {
          value: humanizeFloat(value), // numerical value to display
          change: humanizeChange(change), // text of % change with + or - sign
          direction: toColorDirection(change, isInverse(metricUrn, entities))
        };
      }
    });
    return anomalyInfo;
  }),

  /**
   * Returns any offset that has associated current and change values
   * @type {Array}
   */
  availableOffsets: computed('offsets', 'anomalyInfo', function () {
    const { offsets, anomalyInfo } = getProperties(this, 'offsets', 'anomalyInfo');
    return offsets.filter((offset) => offset in anomalyInfo);
  }),

  /**
   * Returns humanized version of current value
   * @type {String}
   */
  humanizedAnomalyCurrent: computed('current', function () {
    const oldCurrent = get(this, 'current');
    return humanizeFloat(parseFloat(oldCurrent));
  }),

  /**
   * grabs value of new current only when warningValue is toggled
   * @type {string}
   */
  warningChangedTo: computed('warningValue', function () {
    const newCurrent = this._getAggregate('current') * parseFloat(get(this, 'aggregateMultiplier'));
    return humanizeFloat(newCurrent);
  }),

  /**
   * Returns the aggregate value for a given offset. Handles computed baseline special case.
   *
   * @param {string} offset metric offset
   * @return {Float} aggregate value for offset
   * @private
   */
  _getAggregate(offset) {
    const { metricUrn, aggregates, predicted, aggregateMultiplier } = getProperties(
      this,
      'metricUrn',
      'aggregates',
      'predicted',
      'aggregateMultiplier'
    );
    if (offset === 'predicted') {
      const value = parseFloat(predicted);
      if (value === 0.0) {
        return Number.NaN;
      }
      return value / (aggregateMultiplier || 1.0);
    }
    return aggregates[toOffsetUrn(metricUrn, offset)];
  },

  didReceiveAttrs() {
    this._super(...arguments);

    // Don't show warning when granularity is 1 or 5 minutes, regardless of discrepancy
    const metricGranularity = get(this, 'metricGranularity');
    if (metricGranularity === '1_DAYS' && !get(this, 'isRangeChanged')) {
      const oldCurrent = parseFloat(get(this, 'current'));
      let newCurrent = this._getAggregate('current');
      const aggregateMultiplier = parseFloat(get(this, 'aggregateMultiplier'));
      if (newCurrent && oldCurrent && aggregateMultiplier) {
        newCurrent = newCurrent * aggregateMultiplier;
        const diffCurrent = Math.abs((newCurrent - oldCurrent) / newCurrent);
        if (diffCurrent > 0.01) {
          set(this, 'warningValue', true);
        } else {
          set(this, 'warningValue', false);
        }
      }
    }
  },

  actions: {
    /**
     * Handles updates to the anomaly feedback by the user
     *
     * @param {string} status new anomaly feedback status
     */
    onFeedback(status) {
      const { onFeedback, anomalyUrn } = getProperties(this, 'onFeedback', 'anomalyUrn');

      if (onFeedback) {
        onFeedback(anomalyUrn, status);
      }

      // TODO reload anomaly entity instead
      setProperties(this, { status, isHiddenUser: false });
    },

    /**
     * Toggle visibility of anomaly header between hidden/expanded
     */
    toggleHidden() {
      const { isHidden } = getProperties(this, 'isHidden');
      setProperties(this, { isHiddenUser: !isHidden });
    }
  }
});
