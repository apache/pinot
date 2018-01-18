import Component from "@ember/component";
import { computed, setProperties, getProperties, get } from '@ember/object';
import moment from 'moment';
import { humanizeFloat, filterPrefix, toOffsetUrn } from 'thirdeye-frontend/helpers/utils';

const ROOTCAUSE_HIDDEN_DEFAULT = 'default';

/**
 * Maps the status from the db to something human readable to display on the form
 * @type {Object}
 */
const ANOMALY_OPTIONS_MAPPING = {
  'ANOMALY': 'Yes (True Anomaly)',
  'ANOMALY_NEW_TREND': 'Yes (But New Trend)',
  'NOT_ANOMALY': 'No (False Alarm)',
  'NO_FEEDBACK': 'To Be Determined'
};

export default Component.extend({
  classNames: ['rootcause-anomaly'],
  entities: null, // {}

  aggregates: null, // {}

  anomalyUrns: null, // Set

  onFeedback: null, // func (urn, feedback, comment)

  isHiddenUser: ROOTCAUSE_HIDDEN_DEFAULT,

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
      const { anomalyUrns } = getProperties(this, 'anomalyUrns');
      const anomalyEventUrn = filterPrefix(anomalyUrns, 'thirdeye:event:anomaly:');

      if (!anomalyEventUrn) { return; }

      return anomalyEventUrn[0];
    }
  ),

  anomaly: computed(
    'entities',
    'anomalyUrn',
    function () {
      const { entities, anomalyUrn } = getProperties(this, 'entities', 'anomalyUrn');

      if (!anomalyUrn || !entities || !entities[anomalyUrn]) { return false; }

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
    function () {
      const { aggregates, anomalyUrns } = getProperties(this, 'aggregates', 'anomalyUrns');

      const offsets = ['current', 'baseline', 'wo1w', 'wo2w', 'wo3w', 'wo4w'];
      const metricUrns = filterPrefix(anomalyUrns, 'thirdeye:metric:');

      if (!metricUrns) { return; }

      let anomalyInfo = {};

      [...offsets].forEach(offset => {
        const change = aggregates[toOffsetUrn(metricUrns[0], 'current')] / aggregates[toOffsetUrn(metricUrns[0], offset)] - 1;
        const roundedChange = (Math.round(change * 1000) / 10.0).toFixed(1);
        const sign = change > 0 ? '+' : '';

        let offsetFormatted = '';

        switch(offset) {
          case 'baseline':
            offsetFormatted = 'Predicted Average';
            break;
          case 'current':
            offsetFormatted = 'Current Average';
            break;
          default:
            offsetFormatted = offset;
        }

        anomalyInfo[offsetFormatted] = {};
        anomalyInfo[offsetFormatted].change = roundedChange;
        anomalyInfo[offsetFormatted].value = aggregates[toOffsetUrn(metricUrns[0], offset)];
        anomalyInfo[offsetFormatted].changeFormatted = `${sign}${roundedChange}`;
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
      const { externalUrls } = get(this, 'anomaly.attributes');
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
      setProperties(this, { status });
    },

    toggleHidden() {
      const { isHidden } = getProperties(this, 'isHidden');
      setProperties(this, { isHiddenUser: !isHidden });
    }
  }
});
