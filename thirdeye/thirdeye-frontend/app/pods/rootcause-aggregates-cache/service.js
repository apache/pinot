import Ember from 'ember';
import { checkStatus, filterPrefix, toBaselineRange, toFilters, toFilterMap } from 'thirdeye-frontend/helpers/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Service.extend({
  aggregates: null, // {}

  context: null, // {}

  pending: null, // Set

  init() {
    this.setProperties({aggregates: {}, context: {}, pending: {}});
  },

  request(requestContext, urns) {
    console.log('rootcauseAggregatesService: request()', requestContext, urns);
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
      newAggregates = metrics.filter(urn => aggregates[urn]).reduce((agg, urn) => { agg[urn] = aggregates[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !aggregates[urn] && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newAggregates = aggregates;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), aggregates: newAggregates, pending: newPending });

    if (_.isEmpty(missing)) {
      console.log('rootcauseAggregatesService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('frontend:metric:current:'));
    metricUrns.forEach(urn => this._fetchSlice(urn, requestContext.anomalyRange, requestContext));

    // baselines
    const baselineRange = toBaselineRange(requestContext.anomalyRange, requestContext.compareMode);
    const baselineUrns = missing.filter(urn => urn.startsWith('frontend:metric:baseline:'));
    baselineUrns.forEach(urn => this._fetchSlice(urn, baselineRange, requestContext));
  },

  _complete(requestContext, incoming) {
    console.log('rootcauseAggregatesService: _complete()', incoming);
    const { context, pending, aggregates } = this.getProperties('context', 'pending', 'aggregates');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseAggregatesService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !incoming[urn]));
    const newAggregates = Object.assign({}, aggregates, incoming);

    this.setProperties({ aggregates: newAggregates, pending: newPending });
  },

  _extractAggregates(incoming, urn) {
    // NOTE: only supports single time range
    const aggregates = {};
    Object.keys(incoming).forEach(range => {
      Object.keys(incoming[range]).forEach(mid => {
        const aggregate = incoming[range][mid];
        aggregates[urn] = aggregate;
      });
    });
    return aggregates;
  },

  _fetchSlice(urn, range, context) {
    const metricId = urn.split(':')[3];
    const metricFilters = toFilters([urn]);
    const contextFilters = toFilters(filterPrefix(context.urns, 'thirdeye:dimension:'));
    const filters = toFilterMap(metricFilters.concat(contextFilters));

    const filterString = encodeURIComponent(JSON.stringify(filters));

    const url = `/aggregation/aggregate?metricIds=${metricId}&ranges=${range[0]}:${range[1]}&filters=${filterString}`;
    return fetch(url)
    // .then(checkStatus)
      .then(res => res.json())
      .then(res => this._extractAggregates(res, urn))
      .then(res => this._complete(context, res));
  }
});
