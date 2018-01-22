import Component from "@ember/component";
import { computed, setProperties, getProperties, get } from '@ember/object';
import moment from 'moment';
import { humanizeFloat, filterPrefix, toOffsetUrn, humanizeChange, toColorDirection, isInverse } from 'thirdeye-frontend/helpers/utils';

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
    function () {
      const { aggregates, anomalyUrns, entities } =
        getProperties(this, 'aggregates', 'anomalyUrns', 'entities');

      const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

      if (!metricUrns) { return; }

      // NOTE: supports single metric only
      const metricUrn = metricUrns[0];

      const anomalyInfo = {};

      [...this.offsets].forEach(offset => {
        const offsetAggregate = aggregates[toOffsetUrn(metricUrn, offset)];
        const change = aggregates[toOffsetUrn(metricUrn, 'current')] / offsetAggregate - 1;

        anomalyInfo[offset] = {
          value: offsetAggregate, // numerical value to display
          change: humanizeChange(change), // text of % change with + or - sign
          direction: toColorDirection(change, isInverse(metricUrn, entities))
        };
      });

      return anomalyInfo;
    }
  ),

  functionName: computed('anomaly', function () {
    return get(this, 'anomaly').attributes.function[0];
  }),

  anomalyId: computed('anomaly', function () {
    return get(this, 'anomaly').urn.split(':')[3];
  }),

  metric: computed('anomaly', function () {
    return get(this, 'anomaly').attributes.metric[0];
  }),

  dataset: computed('anomaly', function () {
    return get(this, 'anomaly').attributes.dataset[0];
  }),

  status: computed('anomaly', function () {
    return get(this, 'anomaly').attributes.status[0];
  }),

  duration: computed('anomaly', function () {
    const anomaly = get(this, 'anomaly');
    return moment.duration(anomaly.end - anomaly.start).humanize();
  }),

  dimensions: computed('anomaly', function () {
    const attr = get(this, 'anomaly').attributes;
    const dimNames = attr.dimensions || [];
    const dimValues = dimNames.reduce((agg, dimName) => { agg[dimName] = attr[dimName][0]; return agg; }, {});
    return dimNames.sort().map(dimName => dimValues[dimName]).join(', ');
  }),

  issueType: null, // TODO

  metricFormatted: computed(
    'dataset',
    'metric',
    function () {
      const { dataset, metric } =
        getProperties(this, 'dataset', 'metric');
      return `${dataset}::${metric}`;
    }
  ),

  dimensionsFormatted: computed('anomaly', function () {
    const dimensions = get(this, 'dimensions');
    return dimensions ? ` (${dimensions})` : '';
  }),

  changeFormatted: computed('change', function () {
    const change = get(this, 'change');
    const prefix = change > 0 ? '+' : '';
    return `${prefix}${humanizeFloat(change * 100)}%`;
  }),

  startFormatted: computed('anomaly', function () {
    return moment(get(this, 'anomaly').start).format('MMM D YYYY, hh:mm a');
  }),

  endFormatted: computed('anomaly', function () {
    return moment(get(this, 'anomaly').end).format('MMM D YYYY, hh:mm a');
  }),

  requiresFeedback: computed('status', function () {
    return get(this, 'status') === 'NO_FEEDBACK';
  }),

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

  actions: {
    onFeedback(status) {
      const { onFeedback, anomalyUrn } = getProperties(this, 'onFeedback', 'anomalyUrn');

      if (onFeedback) {
        onFeedback(anomalyUrn, status, '');
      }

      // TODO reload anomaly entity instead
      setProperties(this, { status, isHiddenUser: false });
    },

    toggleHidden() {
      const { isHidden } = getProperties(this, 'isHidden');
      setProperties(this, { isHiddenUser: !isHidden });
    }
  }
});
