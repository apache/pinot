import Ember from 'ember';
import { checkStatus, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Service.extend({
  breakdowns: null, // {}

  context: null, // {}

  pending: null, // Set

  init() {
    this.setProperties({breakdowns: {}, context: {}, pending: {}});
  },

  request(requestContext, urns) {
    console.log('rootcauseBreakdownsService: request()', requestContext, urns);
    const { context, breakdowns, pending } = this.getProperties('context', 'breakdowns', 'pending');

    const metrics = [...urns].filter(urn => (urn.startsWith('thirdeye:metric:') || urn.startsWith('frontend:baseline:metric:')));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newBreakdowns;
    if(!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      missing = metrics;
      newPending = new Set(metrics);
      newBreakdowns = metrics.filter(urn => breakdowns[urn]).reduce((agg, urn) => { agg[urn] = breakdowns[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !breakdowns[urn] && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newBreakdowns = breakdowns;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), breakdowns: newBreakdowns, pending: newPending });

    const filtersMap = this._makeFiltersMap(requestContext.urns);
    const filtersString = encodeURIComponent(JSON.stringify(filtersMap));

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('thirdeye:metric:'));
    if (!_.isEmpty(metricUrns)) {
      const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
      const metricUrl = `/aggregation/query?metricIds=${metricIdString}&ranges=${requestContext.anomalyRange[0]}:${requestContext.anomalyRange[1]}&filters=${filtersString}`;

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
      const baselineUrl = `/aggregation/query?metricIds=${baselineIdString}&ranges=${requestContext.baselineRange[0]}:${requestContext.baselineRange[1]}&filters=${filtersString}`;

      fetch(baselineUrl)
         // .then(checkStatus)
        .then(res => res.json())
        .then(res => this._extractAggregates(res, (mid) => `frontend:baseline:metric:${mid}`))
        .then(incoming => this._complete(requestContext, incoming));
    }
  },

  _complete(requestContext, incoming) {
    console.log('rootcauseBreakdownsService: _complete()', incoming);
    const { context, pending, breakdowns } = this.getProperties('context', 'pending', 'breakdowns');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseBreakdownsService: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !incoming[urn]));
    const newBreakdowns = Object.assign({}, breakdowns, incoming);

    this.setProperties({ breakdowns: newBreakdowns, pending: newPending });
  },

  _extractAggregates(incoming, urnFunc) {
    // NOTE: only supports single time range
    const breakdowns = {};
    Object.keys(incoming).forEach(range => {
      Object.keys(incoming[range]).forEach(mid => {
        const breakdown = incoming[range][mid];
        const urn = urnFunc(mid);
        breakdowns[urn] = breakdown;
      });
    });
    return breakdowns;
  },

  _makeFiltersMap(urns) {
    const filters = filterPrefix(urns, 'thirdeye:dimension:').map(urn => { const t = urn.split(':'); return [t[2], t[3]]; });
    return filters.reduce((agg, t) => { if (!agg[t[0]]) { agg[t[0]] = [t[1]]; } else { agg[t[0]] = agg[t[0]].concat(t[1]); } return agg; }, {});
  }
});
