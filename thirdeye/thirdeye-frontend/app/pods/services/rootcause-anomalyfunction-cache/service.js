import Service from '@ember/service';
import {
  toFilters,
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';
import moment from 'moment';

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

    const metrics = [...urns].filter(urn => urn.startsWith('frontend:anomalyfunction:'));

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
    const timestamp = json['timeBuckets'].map(bucket => parseInt(bucket['baselineStart'], 10));
    const value = json['baselineValues'].map(parseFloat);

    const timeseries = {};
    timeseries[urn] = {
      timestamp,
      value
    };

    return timeseries;
  },

  _fetchSlice(urn, context) {
    const functionId = urn.split(':')[2];
    const startDateTime = moment(context.analysisRange[0]).utc().format();
    const endDateTime = moment(context.analysisRange[1]).utc().format();
    const dimensionJsonString = encodeURIComponent(JSON.stringify(this._toFilterMapCustom(toFilters(urn))));

    const url = `/dashboard/anomaly-function/${functionId}/baseline?start=${startDateTime}&end=${endDateTime}&dimension=${dimensionJsonString}&mode=offline`;
    return fetch(url)
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
  },

  _toFilterMapCustom(filters) {
    const filterMap = {};
    [...filters].forEach(tup => {
      const [key, value] = tup;
      filterMap[key] = value;
    });
    return filterMap;
  }
});
