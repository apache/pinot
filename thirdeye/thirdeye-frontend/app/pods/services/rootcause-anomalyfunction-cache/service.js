import Service from '@ember/service';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Service.extend({
  timeseries: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  init() {
    this._super(...arguments);
    this.setProperties({ timeseries: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, timeseries, pending } = this.getProperties('context', 'timeseries', 'pending');

    const metrics = [...urns].filter(urn => urn.startsWith('thirdeye:event:anomaly'));

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
      // console.log('rootcauseAnomalyFunctionService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    missing.forEach(urn => {
      return this._fetchSlice(urn, requestContext);
    });
  },

  _complete(requestContext, incoming) {
    const { context, pending, timeseries } = this.getProperties('context', 'pending', 'timeseries');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseAnomalyFunctionService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !(urn in incoming)));
    const newTimeseries = Object.assign({}, timeseries, incoming);

    this.setProperties({ timeseries: newTimeseries, pending: newPending });
  },

  _extractTimeseries(json, urn) {
    const timestamp = json['timestamp'].map(bucket => parseInt(bucket, 10));
    const value = json['value'].map(parseFloat);

    const timeseries = {};
    timeseries[urn] = {
      timestamp,
      value
    };

    return timeseries;
  },

  _fetchSlice(urn, context) {
    const functionId = urn.split(':')[3];
    const startDateTime = context.analysisRange[0];
    const endDateTime = context.analysisRange[1];

    const url = `/detection/predicted-baseline/${functionId}?start=${startDateTime}&end=${endDateTime}`;

    return fetch(url)
      .then(checkStatus)
      .then(res => this._extractTimeseries(res, urn))
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
  },

  _toFilterMapCustom(filters) {
    const filterMap = {};
    [...filters].forEach(tup => {
      const [key, op, value] = tup;
      filterMap[key] = value;
    });
    return filterMap;
  }
});
