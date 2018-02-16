import Component from "@ember/component";
import {
  computed,
  setProperties,
  getProperties,
  get
} from '@ember/object';
import moment from 'moment';
import {
  filterPrefix,
  toOffsetUrn,
  toColorDirection,
  isInverse,
  toMetricLabel
} from 'thirdeye-frontend/utils/rca-utils';
import { humanizeChange } from 'thirdeye-frontend/utils/utils';
import { equal, reads } from '@ember/object/computed';

const ROOTCAUSE_HIDDEN_DEFAULT = 'default';

const OFFSETS = ['current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];

/**
 * Maps the status from the db to something human readable to display on the form
 * @type {Object}
 */
const ANOMALY_OPTIONS_MAPPING = {
  ANOMALY: 'Yes (True Anomaly)',
  ANOMALY_NEW_TREND: 'Yes (But New Trend)',
  NOT_ANOMALY: 'No (False Alarm)',
  NO_FEEDBACK: 'To Be Determined'
};

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
   * A mapping of the status and a more human readable version
   * @type {Object}
   */
  optionsMapping: ANOMALY_OPTIONS_MAPPING,

  /**
   * Urn of an anomaly
   * @type {String}
   */
  anomalyUrn: computed(
    'anomalyUrns',
    function () {
      const anomalyUrns = get(this, 'anomalyUrns');
      const anomalyEventUrn = filterPrefix(anomalyUrns, 'thirdeye:event:anomaly:');

      if (!anomalyEventUrn) return ;

      return anomalyEventUrn[0];
    }
  ),

  /**
   * Metric urn for anomaly topic metric
   * @type {string}
   */
  metricUrn: computed(
    'anomalyUrns',
    function () {
      const { anomalyUrns } = getProperties(this, 'anomalyUrns');

      const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

      if (!metricUrns) { return; }

      return metricUrns[0];
    }
  ),

  /**
   * Information about an anomaly displayed in the overview regarding its id, metric, dimensions, alert name, and duration
   * @type {Object}
   */
  anomaly: computed(
    'entities',
    'anomalyUrn',
    function () {
      const { entities, anomalyUrn } = getProperties(this, 'entities', 'anomalyUrn');

      if (!anomalyUrn || !entities || !entities[anomalyUrn]) return ;

      return entities[anomalyUrn];
    }
  ),

  /**
   * Anomaly function (alert) name
   * @type {string}
   */
  functionName: reads('anomaly.attributes.function.firstObject'),

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
   * @type {Float}
   */
  baseline: reads('anomaly.attributes.baseline.firstObject'),

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
  metricLabel: computed(
    'anomalyUrns',
    'entities',
    function () {
      const { anomalyUrns, entities } = getProperties(this, 'anomalyUrns', 'entities');

      const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

      if (!metricUrns) { return; }

      const metricUrn = metricUrns[0];

      return toMetricLabel(metricUrn, entities);
    }
  ),

  /**
   * Formatted anomaly start time
   * @type {string}
   */
  startFormatted: computed('anomaly', function () {
    return moment(get(this, 'anomaly').start).format('MMM D YYYY, hh:mm a');
  }),

  /**
   * Formatted anomaly end time
   * @type {string}
   */
  endFormatted: computed('anomaly', function () {
    return moment(get(this, 'anomaly').end).format('MMM D YYYY, hh:mm a');
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
  isHidden: computed(
    'requiresFeedback',
    'isHiddenUser',
    function () {
      const { requiresFeedback, isHiddenUser } = getProperties(this, 'requiresFeedback', 'isHiddenUser');
      if (isHiddenUser === ROOTCAUSE_HIDDEN_DEFAULT) {
        return !requiresFeedback;
      }
      return isHiddenUser;
    }
  ),

  /**
   * External links for current anomaly
   * @type {Object} - an object with key as the link type and value as the url
   */
  anomalyLinks: computed(
    'anomaly',
    function() {
      const { externalUrls = [] } = get(this, 'anomaly.attributes');
      let urls = {};

      if (externalUrls.length) {
        externalUrls.forEach(url => {
          urls[url] = get(this, 'anomaly.attributes')[url][0]; // there will always be only 1 element in this array
        });
      }
      return urls;
    }
  ),

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
  anomalyInfo: computed(
    'aggregates',
    'anomalyUrns',
    'entities',
    'baseline',
    function () {
      const { metricUrn, entities } = getProperties(this, 'metricUrn', 'entities');

      const curr = this._getAggregate('current');

      const anomalyInfo = {};
      [...this.offsets].forEach(offset => {
        const value = this._getAggregate(offset);
        const change = curr / value - 1;

        anomalyInfo[offset] = {
          value, // numerical value to display
          change: humanizeChange(change), // text of % change with + or - sign
          direction: toColorDirection(change, isInverse(metricUrn, entities))
        };
      });

      return anomalyInfo;
    }
  ),

  /**
   * Returns the aggregate value for a given offset. Handles computed baseline special case.
   *
   * @param {string} offset metric offset
   * @return {Float} aggregate value for offset
   * @private
   */
  _getAggregate(offset) {
    const { metricUrn, aggregates, baseline } = getProperties(this, 'metricUrn', 'aggregates', 'baseline');

    if (offset === 'baseline') {
      return baseline;
    }

    return aggregates[toOffsetUrn(metricUrn, offset)];
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
        onFeedback(anomalyUrn, status, '');
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
