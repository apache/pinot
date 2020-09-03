import Service from '@ember/service';
import { inject as service } from '@ember/service';
import {
  toMetricUrn,
  toAbsoluteUrn
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus, stripNonFiniteValues } from 'thirdeye-frontend/utils/utils';
import config from 'thirdeye-frontend/config/environment';
import _ from 'lodash';

const ROOTCAUSE_TIMESERIES_ENDPOINT = '/rootcause/metric/timeseries';
const ROOTCAUSE_TIMESERIES_PRIORITY = 10;

export default Service.extend({
  timeseries: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  fetcher: service('services/rootcause-fetcher'),

  init() {
    this._super(...arguments);
    this.setProperties({ timeseries: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, timeseries, pending } = this.getProperties('context', 'timeseries', 'pending');

    const metrics = [...urns].filter(urn => urn.startsWith('frontend:metric:'));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newTimeseries;
    if(!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      this.get('fetcher').resetPrefix(ROOTCAUSE_TIMESERIES_ENDPOINT);

      missing = metrics;
      newPending = new Set(metrics);
      newTimeseries = metrics.filter(urn => urn in timeseries).reduce((agg, urn) => { agg[urn] = timeseries[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !(urn in timeseries) && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newTimeseries = timeseries;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), timeseries: newTimeseries, pending: newPending });

    if (_.isEmpty(missing)) {
      // console.log('rootcauseTimeseriesService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    [...missing].sort().forEach((urn, i) => {
      return this._fetchSlice(urn, requestContext, i);
    });
  },

  _complete(requestContext, incoming) {
    const { context, pending, timeseries } = this.getProperties('context', 'pending', 'timeseries');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseTimeseriesService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !(urn in incoming)));
    const newTimeseries = Object.assign({}, timeseries, incoming);

    this.setProperties({ timeseries: newTimeseries, pending: newPending });
  },

  _extractTimeseries(json, urn) {
    const timeseries = {};
    json.value = stripNonFiniteValues((json.value || []));
    timeseries[urn] =  json;
    return timeseries;
  },

  _fetchSlice(urn, context) {
    const fetcher = this.get('fetcher');

    const metricUrn = toMetricUrn(urn);
    const range = context.analysisRange;
    const offset = toAbsoluteUrn(urn, context.compareMode).split(':')[2].toLowerCase();
    const granularity = context.granularity;
    const timezone = config.timeZone;

    const url = `${ROOTCAUSE_TIMESERIES_ENDPOINT}?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    return fetcher.fetch(url, ROOTCAUSE_TIMESERIES_PRIORITY)
      .then(checkStatus)
      .then(res => this._extractTimeseries(res, urn))
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
