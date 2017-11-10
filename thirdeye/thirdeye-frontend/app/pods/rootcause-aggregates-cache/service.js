import Ember from 'ember';
import checkStatus from 'thirdeye-frontend/helpers/utils';
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
    const { context, aggregates } = this.getProperties('context', 'aggregates');

    const metrics = [...urns].filter(urn => (urn.startsWith('thirdeye:metric:') || urn.startsWith('frontend:baseline:metric:')));

    // TODO eviction on cache size limit

    let missing;
    let newAggregates;
    if(!_.isEqual(context.anomalyRange, requestContext.anomalyRange)) {
      // new analysis range: evict all, reload
      missing = metrics;
      newAggregates = metrics.filter(urn => aggregates[urn]).reduce((agg, urn) => agg[urn] = aggregates[urn], {});

    } else if(!_.isEqual(context.baselineRange, requestContext.baselineRange)) {
      // new baseline: reload baselines, load missing
      missing = metrics.filter(urn => !aggregates[urn] || urn.startsWith('frontend:baseline:metric:'));
      newAggregates = Object.keys(aggregates)
        .filter(urn => urns.has(urn) || !urn.startsWith('frontend:baseline:metric:'))
        .reduce((agg, urn) => agg[urn] = aggregates[urn], {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !aggregates[urn]);
      newAggregates = aggregates;
    }

    const newPending = new Set(missing);
    this.setProperties({ context: _.cloneDeep(requestContext), aggregates: newAggregates, pending: newPending });

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('thirdeye:metric:'));
    if (!_.isEmpty(metricUrns)) {
      const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
      const metricUrl = `/aggregation/query?metricIds=${metricIdString}&ranges=${requestContext.anomalyRange[0]}:${requestContext.anomalyRange[1]}`;

      fetch(metricUrl)
        // .then(checkStatus)
        .then(res => res.json())
        .then(res => this._extractAggregates(res, (mid) => `thirdeye:metric:${mid}`))
        .then(incoming => this._complete(requestContext, incoming));
    }

    // baselines
    const baselineUrns = missing.filter(urn => urn.startsWith('frontend:baseline:metric:'));
    if (!_.isEmpty(baselineUrns)) {
      const baselineIdString = baselineUrns.map(urn => urn.split(":")[3]).join(',');
      const baselineUrl = `/aggregation/query?metricIds=${baselineIdString}&ranges=${requestContext.baselineRange[0]}:${requestContext.baselineRange[1]}`;

      fetch(baselineUrl)
         // .then(checkStatus)
        .then(res => res.json())
        .then(res => this._extractAggregates(res, (mid) => `frontend:baseline:metric:${mid}`))
        .then(incoming => this._complete(requestContext, incoming));
    }
  },

  _complete(requestContext, incoming) {
    console.log('rootcauseAggregatesService: _complete()', incoming);
    const { context, pending, aggregates } = this.getProperties('context', 'pending', 'aggregates');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseAggregatesService: received stale result. ignoring.');
      return;
    }

    if (_.isEmpty(incoming)) {
      console.log('rootcauseAggregatesService: received empty result.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !incoming[urn]));
    const newAggregates = Object.assign({}, aggregates, incoming);

    this.setProperties({ aggregates: newAggregates, pending: newPending });
  },

  _extractAggregates(incoming, urnFunc) {
    // NOTE: only supports single time range
    const aggregates = {};
    Object.keys(incoming).forEach(range => {
      Object.keys(incoming[range]).forEach(mid => {
        const aggregate = incoming[range][mid];
        const urn = urnFunc(mid);
        aggregates[urn] = aggregate;
      });
    });
    return aggregates;
  }
});
