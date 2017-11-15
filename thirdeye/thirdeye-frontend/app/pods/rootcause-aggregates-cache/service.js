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
    
    const filtersMap = this._makeFiltersMap(requestContext.urns);

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('frontend:metric:current:'));
    if (!_.isEmpty(metricUrns)) {
      metricUrns.forEach(urn => {
        const metricId = urn.split(":")[3];
        const dimensionFragments = _.slice(urn.split(':'), 4).join(':');
        const dimensionString = dimensionFragments ? ':' + dimensionFragments : '';
        const dimensionFilters = encodeURIComponent(JSON.stringify(this._makeDimensionFiltersMap(filtersMap, urn)));

        const url = `/aggregation/aggregate?metricIds=${metricId}&ranges=${requestContext.anomalyRange[0]}:${requestContext.anomalyRange[1]}&filters=${dimensionFilters}`;
        const urnFunc = (mid, ds) => `frontend:metric:current:${mid}${ds}`;
        fetch(url)
          // .then(checkStatus)
          .then(res => res.json())
          .then(res => this._extractAggregates(res, (mid) => urnFunc(mid, dimensionString)))
          .then(incoming => this._complete(requestContext, incoming));
      });
    }

    // baselines
    const baselineUrns = missing.filter(urn => urn.startsWith('frontend:metric:baseline:'));
    if (!_.isEmpty(baselineUrns)) {
      baselineUrns.forEach(urn => {
        const metricId = urn.split(':')[3];
        const dimensionFragments = _.slice(urn.split(':'), 4).join(':');
        const dimensionString = dimensionFragments ? ':' + dimensionFragments : '';
        const dimensionFilters = encodeURIComponent(JSON.stringify(this._makeDimensionFiltersMap(filtersMap, urn)));

        const url = `/aggregation/aggregate?metricIds=${metricId}&ranges=${requestContext.baselineRange[0]}:${requestContext.baselineRange[1]}&filters=${dimensionFilters}`;
        const urnFunc = (mid, ds) => `frontend:metric:baseline:${mid}${ds}`;
        fetch(url)
           // .then(checkStatus)
          .then(res => res.json())
          .then(res => this._extractAggregates(res, (mid) => urnFunc(mid, dimensionString)))
          .then(incoming => this._complete(requestContext, incoming));
      });
    }
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
  },

  _makeDimensionFiltersMap(filtersMap, urn) {
    const filters = _.cloneDeep(filtersMap);

    // frontend:dimension:metric:12345:key=value:otherKey=otherValue
    const encodedDimensions = _.slice(urn.split(':'), 4);
    encodedDimensions.forEach(enc => {
      const [key, value] = enc.split('=');
      if (filters[key]) {
        filters[key].push(value);
      } else {
        filters[key] = [value];
      }
    });

    return filters;
  }
});
