import Ember from 'ember';
import checkStatus from 'thirdeye-frontend/helpers/utils';
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
    const { context, timeseries: currTimeseries } = this.getProperties('context', 'timeseries');

    // TODO retain stale timeseries until request completion

    let timeseries = currTimeseries;
    if(!_.isEqual(context.analysisRange, requestContext.analysisRange)) {
      // new analysis range: evict all, leave stale copy of requested
      timeseries = [...urns]
        .filter(urn => timeseries[urn])
        .reduce((agg, urn) => agg[urn] = timeseries[urn], {});
    } else if((context.anomalyRange[0] - context.baselineRange[0]) !=
              (requestContext.anomalyRange[0] - requestContext.baselineRange[0])) {
      // new baseline: evict baselines, leave current
      timeseries = Object.keys(currTimeseries)
        .filter(urn => !urn.startsWith('frontend:baseline:metric:'))
        .reduce((agg, urn) => agg[urn] = currTimeseries[urn], {});
    }

    const missing = [...urns].filter(urn => (urn.startsWith('thirdeye:metric:') || urn.startsWith('frontend:baseline:metric:')) && !timeseries[urn]);

    const newPending = new Set(missing);
    this.setProperties({ context: _.cloneDeep(requestContext), timeseries, pending: newPending });

    // metrics
    const metricUrns = missing.filter(urn => urn.startsWith('thirdeye:metric:'));
    if (!_.isEmpty(metricUrns)) {
      const metricIdString = metricUrns.map(urn => urn.split(":")[2]).join(',');
      const metricUrl = `/timeseries/query?metricIds=${metricIdString}&ranges=${requestContext.analysisRange[0]}:${requestContext.analysisRange[1]}&granularity=15_MINUTES&transformations=timestamp`;

      fetch(metricUrl)
        // .then(checkStatus)
        .then(res => res.json())
        .then(this._extractTimeseries)
        .then(incoming => this._complete(requestContext, incoming));
    }

    // baselines
    const baselineOffset = requestContext.anomalyRange[0] - requestContext.baselineRange[0];
    const baselineAnalysisStart = requestContext.analysisRange[0] - baselineOffset;
    const baselineAnalysisEnd = requestContext.analysisRange[1] - baselineOffset;

    const baselineUrns = missing.filter(urn => urn.startsWith('frontend:baseline:metric:'));
    if (!_.isEmpty(baselineUrns)) {
      const baselineIdString = baselineUrns.map(urn => urn.split(":")[3]).join(',');
      const baselineUrl = `/timeseries/query?metricIds=${baselineIdString}&ranges=${baselineAnalysisStart}:${baselineAnalysisEnd}&granularity=15_MINUTES&transformations=timestamp`;

      fetch(baselineUrl)
        // .then(checkStatus)
        .then(res => res.json())
        .then(this._extractTimeseries)
        .then(incoming => this._convertToBaseline(incoming, baselineOffset))
        .then(incoming => this._complete(requestContext, incoming));
    }
  },

  _complete(requestContext, incoming) {
    console.log('rootcauseTimeseriesService: _complete()', incoming);
    const { context, pending, timeseries } = this.getProperties('context', 'pending', 'timeseries');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseTimeseriesService: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !incoming[urn]));
    const newTimeseries = Object.assign({}, timeseries, incoming);

    this.setProperties({ timeseries: newTimeseries, pending: newPending });
  },

  _extractTimeseries(json) {
    console.log('rootcauseTimeseriesService: _extractTimeseries()', json);
    const timeseries = {};
    Object.keys(json).forEach(range =>
      Object.keys(json[range]).filter(sid => sid != 'timestamp').forEach(sid => {
        const urn = `thirdeye:metric:${sid}`;
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
      const baselineUrn = this._makeBaselineUrn(urn);
      baseline[baselineUrn] = {
        values: timeseries[urn].values,
        timestamps: timeseries[urn].timestamps.map(t => t + offset)
      };
    });
    return baseline;
  },

  _makeBaselineUrn(urn) {
    const mid = urn.split(':')[2];
    return `frontend:baseline:metric:${mid}`;
  }

});
