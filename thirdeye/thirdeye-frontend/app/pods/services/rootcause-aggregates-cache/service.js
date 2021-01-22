import Service from '@ember/service';
import { inject as service } from '@ember/service';
import { toOffsetUrn, toMetricUrn, toAbsoluteUrn } from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import config from 'thirdeye-frontend/config/environment';

import _ from 'lodash';

const ROOTCAUSE_AGGREGATES_ENDPOINT = '/rootcause/metric/aggregate/chunk';
const ROOTCAUSE_AGGREGATES_PRIORITY = 20;
const ROOTCAUSE_AGGREGATES_CHUNK_SIZE = 10;

export default Service.extend({
  aggregates: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  fetcher: service('services/rootcause-fetcher'),

  init() {
    this._super(...arguments);
    this.setProperties({ aggregates: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, aggregates, pending } = this.getProperties('context', 'aggregates', 'pending');

    const metrics = [...urns].filter((urn) => urn.startsWith('frontend:metric:'));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newAggregates;
    if (!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      this.get('fetcher').resetPrefix(ROOTCAUSE_AGGREGATES_ENDPOINT);

      missing = metrics;
      newPending = new Set(metrics);
      newAggregates = metrics
        .filter((urn) => urn in aggregates)
        .reduce((agg, urn) => {
          agg[urn] = aggregates[urn];
          return agg;
        }, {});
    } else {
      // same context: load missing
      missing = metrics.filter((urn) => !(urn in aggregates) && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newAggregates = aggregates;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), aggregates: newAggregates, pending: newPending });

    if (_.isEmpty(missing)) {
      return;
    }

    // group by metrics and offsets
    const groupedByUrn = [...missing]
      .map((urn) => {
        let absoluteUrn = toAbsoluteUrn(urn, requestContext.compareMode);
        return { absoluteUrn, base: toMetricUrn(absoluteUrn), offset: absoluteUrn.split(':')[2] };
      })
      .reduce((agg, obj) => {
        agg[obj.base] = agg[obj.base] || new Set();
        agg[obj.base].add(obj.offset);
        return agg;
      }, {});

    // workaround for JS conversion of key values to strings
    const setsOfOffsets = Object.keys(groupedByUrn).reduce((agg, urn) => {
      const offsets = [...groupedByUrn[urn]].sort();
      const key = offsets.join('_');
      agg[key] = new Set(offsets);
      return agg;
    }, {});

    // hack baseline translation
    const baselineOffset = requestContext.compareMode === 'WoW' ? 'wo1w' : requestContext.compareMode.toLowerCase();

    Object.values(setsOfOffsets).forEach((offsets) => {
      const urns = Object.keys(groupedByUrn).filter((urn) => _.isEqual(groupedByUrn[urn], offsets));
      const chunks = _.chunk(urns.sort(), ROOTCAUSE_AGGREGATES_CHUNK_SIZE);
      chunks.forEach((urns, i) => {
        this._fetchChunk(urns, [...offsets].sort(), baselineOffset, requestContext, i);
      });
    });
  },

  /**
   * Fetch the metric data for a row of the metric table
   *
   * @param {Array} metricUrns Metric urns
   * @param {Array} offsets time offsets
   * @param {string} baselineOffset offset for baseline translation
   * @param {Object} requestContext Context
   * @returns {undefined}
   */
  async _fetchChunk(metricUrns, offsets, baselineOffset, requestContext, index) {
    const fetcher = this.get('fetcher');

    const [start, end] = requestContext.anomalyRange;
    const timezone = config.timeZone;

    const url = `${ROOTCAUSE_AGGREGATES_ENDPOINT}?urns=${encodeURIComponent(
      metricUrns
    )}&start=${start}&end=${end}&offsets=${offsets}&timezone=${timezone}`;
    try {
      const payload = await fetcher.fetch(url, ROOTCAUSE_AGGREGATES_PRIORITY, index);
      const json = await checkStatus(payload);
      const aggregates = this._extractAggregatesChunk(json, metricUrns, offsets, baselineOffset);
      this._complete(requestContext, aggregates);
    } catch (error) {
      const urns = metricUrns.reduce((agg, metricUrn) => {
        return agg.concat([...offsets, 'baseline'].map((offset) => toOffsetUrn(metricUrn, offset)));
      }, []);
      this._handleErrorBatch(urns, error);
    }
  },

  _handleErrorBatch(urns, error) {
    urns.forEach((urn) => this._handleError(urn, error));
  },

  _extractAggregatesChunk(incoming, metricUrns, offsets, baselineOffset) {
    const aggregates = {};
    metricUrns.forEach((metricUrn) => {
      offsets.forEach((offset, i) => {
        const urn = toOffsetUrn(metricUrn, offset);
        aggregates[urn] = incoming[metricUrn][i];

        // duplicate absolute offset as baseline value
        if (offset === baselineOffset) {
          const baselineUrn = toOffsetUrn(metricUrn, 'baseline');
          aggregates[baselineUrn] = aggregates[urn];
        }
      });
    });

    return aggregates;
  },

  _complete(requestContext, incoming) {
    const { context, pending, aggregates } = this.getProperties('context', 'pending', 'aggregates');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseAggregatesService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter((urn) => !(urn in incoming)));
    const newAggregates = Object.assign({}, aggregates, incoming);

    this.setProperties({ aggregates: newAggregates, pending: newPending });
  },

  _extractAggregates(incoming, urn) {
    const aggregates = {};
    aggregates[urn] = incoming;
    return aggregates;
  },

  _handleError(urn) {
    const { errors, pending } = this.getProperties('errors', 'pending');

    const newError = urn;
    const newErrors = new Set([...errors, newError]);

    const newPending = new Set(pending);
    newPending.delete(urn);

    this.setProperties({ errors: newErrors, pending: newPending });
  }
});
