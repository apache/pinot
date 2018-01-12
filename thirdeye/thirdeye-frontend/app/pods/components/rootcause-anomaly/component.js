import Component from "@ember/component";
import { computed, setProperties, getProperties, get } from '@ember/object';
import moment from 'moment';
import { humanizeFloat } from 'thirdeye-frontend/helpers/utils';

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

  anomalyUrn: null, // ""

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

  anomaly: computed(
    'entities',
    'anomalyUrn',
    function () {
      const { entities, anomalyUrn } = getProperties('entities', 'anomalyUrn');

      if (!anomalyUrn || !entities || !entities[anomalyUrn]) { return false; }

      return entities[anomalyUrn];
    }
  ),

  functionName: computed('anomaly', function () {
    return get('anomaly').attributes.function[0];
  }),

  anomalyId: computed('anomaly', function () {
    return get('anomaly').urn.split(':')[3];
  }),

  metric: computed('anomaly', function () {
    return get('anomaly').attributes.metric[0];
  }),

  dataset: computed('anomaly', function () {
    return get('anomaly').attributes.dataset[0];
  }),

  current: computed('anomaly', function () {
    return parseFloat(get('anomaly').attributes.current[0]).toFixed(3);
  }),

  baseline: computed('anomaly', function () {
    return parseFloat(get('anomaly').attributes.baseline[0]).toFixed(3);
  }),

  change: computed('anomaly', function () {
    const attr = get('anomaly').attributes;
    return (parseFloat(attr.current[0]) / parseFloat(attr.baseline[0]) - 1);
  }),

  status: computed('anomaly', function () {
    return get('anomaly').attributes.status[0];
  }),

  duration: computed('anomaly', function () {
    const anomaly = get('anomaly');
    return moment.duration(anomaly.end - anomaly.start).humanize();
  }),

  dimensions: computed('anomaly', function () {
    const attr = get('anomaly').attributes;
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
        getProperties('dataset', 'metric');
      return `${dataset}::${metric}`;
    }
  ),

  dimensionsFormatted: computed('anomaly', function () {
    const dimensions = get('dimensions');
    return dimensions ? ` (${dimensions})` : '';
  }),

  changeFormatted: computed('change', function () {
    const change = get('change');
    const prefix = change > 0 ? '+' : '';
    return `${prefix}${humanizeFloat(change * 100)}%`;
  }),

  startFormatted: computed('anomaly', function () {
    return moment(get('anomaly').start).format('MMM D YYYY, hh:mm a');
  }),

  endFormatted: computed('anomaly', function () {
    return moment(get('anomaly').end).format('MMM D YYYY, hh:mm a');
  }),

  isHidden: computed(
    'requiresFeedback',
    'isHiddenUser',
    function () {
      const { requiresFeedback, isHiddenUser } = getProperties('requiresFeedback', 'isHiddenUser');
      if (isHiddenUser === ROOTCAUSE_HIDDEN_DEFAULT) {
        return !requiresFeedback;
      }
      return isHiddenUser;
    }
  ),

  actions: {
    onFeedback(status) {
      const { onFeedback, anomalyUrn } = getProperties('onFeedback', 'anomalyUrn');

      if (onFeedback) {
        onFeedback(anomalyUrn, status, '');
      }

      // TODO reload anomaly entity instead
      setProperties({ status });
    },

    toggleHidden() {
      const { isHidden } = getProperties('isHidden');
      setProperties({ isHiddenUser: !isHidden });
    }
  }
});
