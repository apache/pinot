import Service from '@ember/service';
import { inject as service } from '@ember/service';
import {
  toMetricUrn,
  toAbsoluteUrn
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_BREAKDOWNS_ENDPOINT = '/rootcause/metric/breakdown';
const ROOTCAUSE_BREAKDOWNS_PRIORITY = 15;

export default Service.extend({
  breakdowns: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  fetcher: service('services/rootcause-fetcher'),

  init() {
    this._super(...arguments);
    this.setProperties({breakdowns: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, breakdowns, pending } = this.getProperties('context', 'breakdowns', 'pending');

    const metrics = [...urns].filter(urn => urn.startsWith('frontend:metric:'));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newBreakdowns;
    if(!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      missing = metrics;
      newPending = new Set(metrics);
      newBreakdowns = metrics.filter(urn => urn in breakdowns).reduce((agg, urn) => { agg[urn] = breakdowns[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !(urn in breakdowns) && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newBreakdowns = breakdowns;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), breakdowns: newBreakdowns, pending: newPending });

    if (_.isEmpty(missing)) {
      // console.log('rootcauseBreakdownsService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    missing.sort().forEach((urn, i) => {
      return this._fetchSlice(urn, requestContext, i);
    });
  },

  _complete(requestContext, incoming) {
    const { context, pending, breakdowns } = this.getProperties('context', 'pending', 'breakdowns');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseBreakdownsService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !(urn in incoming)));
    const newBreakdowns = Object.assign({}, breakdowns, incoming);

    this.setProperties({ breakdowns: newBreakdowns, pending: newPending });
  },

  _extractBreakdowns(incoming, urn) {
    const breakdowns = {};
    breakdowns[urn] = incoming;
    return breakdowns;
  },

  _fetchSlice(urn, context, index) {
    const fetcher = this.get('fetcher');

    const metricUrn = toMetricUrn(urn);
    const range = context.anomalyRange;
    const offset = toAbsoluteUrn(urn, context.compareMode).split(':')[2].toLowerCase();
    const timezone = 'America/Los_Angeles';
    const limit = offset === 'current' ? 100 : 200; // heuristically over-fetch baseline for heat map

    const url = `${ROOTCAUSE_BREAKDOWNS_ENDPOINT}?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&timezone=${timezone}&limit=${limit}`;
    return fetcher.fetch(url, ROOTCAUSE_BREAKDOWNS_PRIORITY, index)
      .then(checkStatus)
      .then(res => this._extractBreakdowns(res, urn))
      .then(res => this._complete(context, res))
      .catch(error => this._handleError(urn, error));
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
