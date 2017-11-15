import Ember from 'ember';
import { checkStatus, toBaselineUrn, filterPrefix } from 'thirdeye-frontend/helpers/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Service.extend({
  timeseries: null, // {}

  context: null, // {}

  pending: null, // Set

  init() {
    this.setProperties({ timeseries: {}, context: {}, pending: new Set() });
  },

  request(requestContext, urns) {
    console.log('rootcauseTimeseriesService: request()', requestContext, urns);
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
      newTimeseries = metrics.filter(urn => timeseries[urn]).reduce((agg, urn) => { agg[urn] = timeseries[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !timeseries[urn] && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newTimeseries = timeseries;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), timeseries: newTimeseries, pending: newPending });

    if (_.isEmpty(missing)) {
      console.log('rootcauseTimeseriesService: request: all metrics up-to-date. ignoring.');
      return;
    }

    const filtersMap = this._makeFiltersMap(requestContext.urns);

    // metrics
    const metricUrns = filterPrefix(missing, 'frontend:metric:current:');
    if (!_.isEmpty(metricUrns)) {
      metricUrns.forEach(urn => {
        const metricId = urn.split(":")[3];
        const dimensionFragments = _.slice(urn.split(':'), 4).join(':');
        const dimensionString = dimensionFragments ? ':' + dimensionFragments : '';
        const dimensionFilters = encodeURIComponent(JSON.stringify(this._makeDimensionFiltersMap(filtersMap, urn)));

        const urnFunc = (mid, ds) => `frontend:metric:current:${mid}${ds}`;
        const url = `/timeseries/query?metricIds=${metricId}&ranges=${requestContext.analysisRange[0]}:${requestContext.analysisRange[1]}&filters=${dimensionFilters}&granularity=${requestContext.granularity}`;
        fetch(url)
          // .then(checkStatus)
          .then(res => res.json())
          .then(json => this._extractTimeseries(json, (mid) => urnFunc(mid, dimensionString)))
          .then(incoming => this._complete(requestContext, incoming));
      });
    }

    // baselines
    const baselineOffset = requestContext.anomalyRange[0] - requestContext.baselineRange[0];
    const baselineAnalysisStart = requestContext.analysisRange[0] - baselineOffset;
    const baselineAnalysisEnd = requestContext.analysisRange[1] - baselineOffset;

    const baselineUrns = filterPrefix(missing, 'frontend:metric:baseline:');
    if (!_.isEmpty(baselineUrns)) {
      baselineUrns.forEach(urn => {
        const metricId = urn.split(":")[3];
        const dimensionFragments = _.slice(urn.split(':'), 4).join(':');
        const dimensionString = dimensionFragments ? ':' + dimensionFragments : '';
        const dimensionFilters = encodeURIComponent(JSON.stringify(this._makeDimensionFiltersMap(filtersMap, urn)));

        const urnFunc = (mid, ds) => `frontend:metric:baseline:${mid}${ds}`;
        const url = `/timeseries/query?metricIds=${metricId}&ranges=${baselineAnalysisStart}:${baselineAnalysisEnd}&filters=${dimensionFilters}&granularity=${requestContext.granularity}`;
        fetch(url)
          // .then(checkStatus)
          .then(res => res.json())
          .then(json => this._extractTimeseries(json, (mid) => urnFunc(mid, dimensionString)))
          .then(incoming => this._convertToBaseline(incoming, baselineOffset))
          .then(incoming => this._complete(requestContext, incoming));
      });
    }
  },

  _complete(requestContext, incoming) {
    console.log('rootcauseTimeseriesService: _complete()', incoming);
    const { context, pending, timeseries } = this.getProperties('context', 'pending', 'timeseries');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseTimeseriesService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !incoming[urn]));
    const newTimeseries = Object.assign({}, timeseries, incoming);

    this.setProperties({ timeseries: newTimeseries, pending: newPending });
  },

  _extractTimeseries(json, urnFunc) {
    console.log('rootcauseTimeseriesService: _extractTimeseries()', json);
    const timeseries = {};
    Object.keys(json).forEach(range =>
      Object.keys(json[range]).filter(sid => sid != 'timestamp').forEach(sid => {
        const urn = urnFunc(sid);
        const jrng = json[range];
        const jval = jrng[sid];

        const timestamps = [];
        const values = [];
        jrng.timestamp.forEach((t, i) => {
          if (jval[i] != null) {
            timestamps.push(t);
            values.push(jval[i]);
          }
        });

        timeseries[urn] = {
          timestamps: timestamps,
          values: values
        };
      })
    );
    return timeseries;
  },

  _convertToBaseline(timeseries, offset) {
    const baseline = {};
    Object.keys(timeseries).forEach(urn => {
      const baselineUrn = toBaselineUrn(urn);
      baseline[baselineUrn] = {
        values: timeseries[urn].values,
        timestamps: timeseries[urn].timestamps.map(t => t + offset)
      };
    });
    return baseline;
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
