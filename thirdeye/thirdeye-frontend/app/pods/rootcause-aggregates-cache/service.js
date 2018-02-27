import Service from '@ember/service';
import {
  toAbsoluteRange,
  toFilters,
  toFilterMap
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Service.extend({
  aggregates: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  init() {
    this.setProperties({aggregates: {}, context: {}, pending: new Set(), errors: new Set() });
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
      // console.log('rootcauseAggregatesService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    missing.forEach(urn => {
      const range = toAbsoluteRange(urn, requestContext.anomalyRange, requestContext.compareMode);
      return this._fetchSlice(urn, range, requestContext);
    });
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
    // NOTE: only supports single time range
    const aggregates = {};
    aggregates[urn] = Number.NaN; // default

    Object.keys(incoming).forEach(range => {
      Object.keys(incoming[range]).forEach(mid => {
        aggregates[urn] = incoming[range][mid];
      });
    });
    return aggregates;
  },

  _fetchSlice(urn, range, context) {
    const metricId = urn.split(':')[3];
    const metricFilters = toFilters([urn]);
    const filters = toFilterMap(metricFilters);
    const filterString = encodeURIComponent(JSON.stringify(filters));

    const url = `/aggregation/aggregate?metricIds=${metricId}&ranges=${range[0]}:${range[1]}&filters=${filterString}`;
    return fetch(url)
      .then(checkStatus)
      .then(res => this._extractAggregates(res, urn))
      .then(res => this._complete(context, res))
      .catch(error => this._handleError(urn, error));
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
