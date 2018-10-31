import Service from '@ember/service';
import { inject as service } from '@ember/service';
import {
  toAbsoluteUrn,
  toMetricUrn
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_AGGREGATES_ENDPOINT = '/rootcause/metric/aggregate/batch';
const ROOTCAUSE_AGGREGATES_PRIORITY = 20;

export default Service.extend({
  aggregates: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  fetcher: service('services/rootcause-fetcher'),

  init() {
    this._super(...arguments);
    this.setProperties({aggregates: {}, context: {}, pending: new Set(), errors: new Set()});
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, aggregates, pending } = this.getProperties('context', 'aggregates', 'pending');

    const metrics = [...urns].filter(urn => urn.startsWith('frontend:metric:'));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newAggregates;
    if(!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      this.get('fetcher').resetPrefix(ROOTCAUSE_AGGREGATES_ENDPOINT);

      missing = metrics;
      newPending = new Set(metrics);
      newAggregates = metrics.filter(urn => urn in aggregates).reduce((agg, urn) => { agg[urn] = aggregates[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !(urn in aggregates) && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newAggregates = aggregates;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), aggregates: newAggregates, pending: newPending });

    if (_.isEmpty(missing)) {
      return;
    }

    const metricUrnToOffestAndUrn = {};
    missing.forEach(urn => {
      const metricUrn = toMetricUrn(urn);
      const offsetsAndUrns = metricUrnToOffestAndUrn[metricUrn] || [];
      offsetsAndUrns.push([toAbsoluteUrn(urn, requestContext.compareMode).split(':')[2].toLowerCase(), urn]);
      metricUrnToOffestAndUrn[metricUrn] = offsetsAndUrns;
    });

    [...Object.keys(metricUrnToOffestAndUrn)].sort().forEach((metricUrn, i) => {
      this._fetchRowSlice(metricUrn, requestContext, metricUrnToOffestAndUrn, i);
    });
  },

  /**
   * Fetch the metric data for a row of the metric table
   *
   * @param {String} metricUrn Metric urn
   * @param {Object} context Context
   * @param {Object} metricUrnToOffestAndUrn Hash map from metric urn to offset and urn
   * @returns {undefined}
   */
  async _fetchRowSlice(metricUrn, requestContext, metricUrnToOffestAndUrn, index) {
    const fetcher = this.get('fetcher');

    const [ start, end ] = requestContext.anomalyRange;
    const offsets = metricUrnToOffestAndUrn[metricUrn].map(tuple => tuple[0]);
    const urns = metricUrnToOffestAndUrn[metricUrn].map(tuple => tuple[1]);
    const timezone = 'America/Los_Angeles';

    const url = `${ROOTCAUSE_AGGREGATES_ENDPOINT}?urn=${metricUrn}&start=${start}&end=${end}&offsets=${offsets}&timezone=${timezone}`;
    try {
      const payload = await fetcher.fetch(url, ROOTCAUSE_AGGREGATES_PRIORITY, index);
      const json = await checkStatus(payload);
      const aggregates = this._extractAggregatesBatch(json, urns);
      this._complete(requestContext, aggregates);

    } catch (error) {
      this._handleErrorBatch(urns, error);
    }
  },

  _handleErrorBatch(urns, error) {
    urns.forEach(urn => this._handleError(urn, error));
  },

  _extractAggregatesBatch(incoming, urns) {
    const aggregates = {};
    for (var i = 0; i < urns.length; i++) {
      aggregates[urns[i]] = incoming[i];
    }

    return aggregates;
  },

  _complete(requestContext, incoming) {
    const { context, pending, aggregates } = this.getProperties('context', 'pending', 'aggregates');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseAggregatesService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !(urn in incoming)));
    const newAggregates = Object.assign({}, aggregates, incoming);

    this.setProperties({ aggregates: newAggregates, pending: newPending });
  },

  _extractAggregates(incoming, urn) {
    const aggregates = {};
    aggregates[urn] = incoming;
    return aggregates;
  },

  _handleError(urn, error) {
    const { errors, pending } = this.getProperties('errors', 'pending');

    const newError = urn;
    const newErrors = new Set([...errors, newError]);

    const newPending = new Set(pending);
    newPending.delete(urn);

    this.setProperties({ errors: newErrors, pending: newPending });
  }
});
