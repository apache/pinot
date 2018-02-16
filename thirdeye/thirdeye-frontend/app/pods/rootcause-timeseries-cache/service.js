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
  timeseries: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  init() {
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
    missing.forEach(urn => {
      const range = toAbsoluteRange(urn, requestContext.analysisRange, requestContext.compareMode);
      const offset = requestContext.analysisRange[0] - range[0];
      const offsetFunc = (ts) => this._offsetTimeseries(ts, offset);
      return this._fetchSlice(urn, range, requestContext, offsetFunc);
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
    Object.keys(json).forEach(range =>
      Object.keys(json[range]).filter(sid => sid != 'timestamp').forEach(sid => {
        const jrng = json[range];
        timeseries[urn] = {
          timestamps: jrng.timestamp,
          values: jrng[sid]
        };
      })
    );
    return timeseries;
  },

  _offsetTimeseries(timeseries, offset) {
    Object.keys(timeseries).forEach(urn => {
      timeseries[urn].timestamps = timeseries[urn].timestamps.map(t => t + offset);
    });
    return timeseries;
  },

  _fetchSlice(urn, range, context, offsetFunc) {
    const metricId = urn.split(':')[3];
    const metricFilters = toFilters([urn]);
    const filters = toFilterMap(metricFilters);

    const filterString = encodeURIComponent(JSON.stringify(filters));

    const url = `/timeseries/query?metricIds=${metricId}&ranges=${range[0]}:${range[1]}&filters=${filterString}&granularity=${context.granularity}`;
    return fetch(url)
      .then(checkStatus)
      .then(res => this._extractTimeseries(res, urn))
      .then(res => offsetFunc(res))
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
