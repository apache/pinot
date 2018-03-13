import Service from '@ember/service';
import {
  toAbsoluteUrn,
  toMetricUrn
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';
import moment from 'moment';

export default Service.extend({
  aggregates: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  init() {
    this._super(...arguments);
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

    let metricUrnToOffestAndUrn = {};
    missing.forEach(urn => {
      const metricUrn = toMetricUrn(urn);
      if (!(metricUrn in metricUrnToOffestAndUrn)) {
        metricUrnToOffestAndUrn[metricUrn] = [];
      }
      metricUrnToOffestAndUrn[metricUrn].push([toAbsoluteUrn(urn, context.compareMode).split(':')[2].toLowerCase(), urn]);
    });

    Object.keys(metricUrnToOffestAndUrn).forEach(
        metricUrn => {
          return this._fetchRowSlice(metricUrn, requestContext, metricUrnToOffestAndUrn);
        });
  },

  _fetchRowSlice(metricUrn, context, metricUrnToOffestAndUrn){
        const range = context.anomalyRange;
        const offsets = metricUrnToOffestAndUrn[metricUrn].map(tuple => tuple[0]);
        const urns = metricUrnToOffestAndUrn[metricUrn].map(tuple => tuple[1]);
        const timezone = moment.tz.guess();
        const url = `/rootcause/metric/aggregate/batch?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offsets=${offsets}&timezone=${timezone}`;
        return fetch(url)
        .then(checkStatus)
        .then(res => this._extractAggregatesBatch(res, urns))
        .then(res => this._complete(context, res))
        .catch(error => this._handleError(urn, error));
  },

  _extractAggregatesBatch(incoming, urns) {
    const aggregates = {};
    for(var i = 0; i < urns.length; i++){
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

  _fetchSlice(urn, context) {
    const metricUrn = toMetricUrn(urn);
    const range = context.anomalyRange;
    const offset = toAbsoluteUrn(urn, context.compareMode).split(':')[2].toLowerCase();
    const timezone = moment.tz.guess();
    const url = `/rootcause/metric/aggregate?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&timezone=${timezone}`;
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
