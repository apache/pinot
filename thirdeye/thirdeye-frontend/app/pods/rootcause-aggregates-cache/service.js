import Ember from 'ember';
import { checkStatus, filterPrefix } from 'thirdeye-frontend/helpers/utils';
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

    const metrics = [...urns].filter(urn => (urn.startsWith('thirdeye:metric:') || urn.startsWith('frontend:baseline:metric:')));

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

    const filtersMap = this._makeFiltersMap(requestContext.urns);
    const filtersString = encodeURIComponent(JSON.stringify(filtersMap));

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('thirdeye:metric:'));
    if (!_.isEmpty(metricUrns)) {
      const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
      const metricUrl = `/aggregation/aggregate?metricIds=${metricIdString}&ranges=${requestContext.anomalyRange[0]}:${requestContext.anomalyRange[1]}&filters=${filtersString}`;

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
      const baselineUrl = `/aggregation/aggregate?metricIds=${baselineIdString}&ranges=${requestContext.baselineRange[0]}:${requestContext.baselineRange[1]}&filters=${filtersString}`;

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
  },

  _makeFiltersMap(urns) {
    const filters = filterPrefix(urns, 'thirdeye:dimension:').map(urn => { const t = urn.split(':'); return [t[2], t[3]]; });
    return filters.reduce((agg, t) => { if (!agg[t[0]]) { agg[t[0]] = [t[1]]; } else { agg[t[0]] = agg[t[0]].concat(t[1]); } return agg; }, {});
  }
});
