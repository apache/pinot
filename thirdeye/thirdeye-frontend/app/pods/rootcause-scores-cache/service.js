import Ember from 'ember';
import { trimTimeRanges, filterPrefix, toBaselineRange } from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Service.extend({
  scores: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  init() {
    this.setProperties({ scores: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, scores, pending } = this.getProperties('context', 'scores', 'pending');

    const metrics = [...urns].filter(urn => urn.startsWith('thirdeye:metric:'));

    // TODO eviction on cache size limit

    let missing;
    let newPending;
    let newScores;
    if(!_.isEqual(context, requestContext)) {
      // new analysis range: evict all, reload, keep stale copy of incoming
      missing = metrics;
      newPending = new Set(metrics);
      newScores = metrics.filter(urn => urn in scores).reduce((agg, urn) => { agg[urn] = scores[urn]; return agg; }, {});

    } else {
      // same context: load missing
      missing = metrics.filter(urn => !(urn in scores) && !pending.has(urn));
      newPending = new Set([...pending].concat(missing));
      newScores = scores;
    }

    this.setProperties({ context: _.cloneDeep(requestContext), scores: newScores, pending: newPending });

    if (_.isEmpty(missing)) {
      // console.log('rootcauseScoresService: request: all metrics up-to-date. ignoring.');
      return;
    }

    // metrics
    fetch(this._makeUrl('metricAnalysis', requestContext, missing))
      .then(checkStatus)
      .then(res => this._extractScores(res, missing))
      .then(res => this._complete(requestContext, res))
      .catch(error => this._handleError(missing, error));
  },

  _complete(requestContext, incoming) {
    const { context, pending, scores } = this.getProperties('context', 'pending', 'scores');

    // only accept latest result
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseScoresService: _complete: received stale result. ignoring.');
      return;
    }

    const newPending = new Set([...pending].filter(urn => !(urn in incoming)));
    const newScores = Object.assign({}, scores, incoming);

    this.setProperties({ scores: newScores, pending: newPending });
  },

  _makeUrl(framework, context, urns) {
    const urnString = filterPrefix(urns, 'thirdeye:metric:').map(encodeURIComponent).join(',');
    const ranges = trimTimeRanges(context.anomalyRange, context.analysisRange);

    const baselineRange = toBaselineRange(ranges.anomalyRange, context.compareMode);
    return `/rootcause/query?framework=${framework}` +
      `&anomalyStart=${ranges.anomalyRange[0]}&anomalyEnd=${ranges.anomalyRange[1]}` +
      `&baselineStart=${baselineRange[0]}&baselineEnd=${baselineRange[1]}` +
      `&analysisStart=${ranges.analysisRange[0]}&analysisEnd=${ranges.analysisRange[1]}` +
      `&urns=${urnString}`;
  },

  _extractScores(res, urns) {
    if (_.isEmpty(res)) {
      return {};
    }
    const template = [...urns].reduce((agg, urn) => {
      agg[urn] = Number.NaN;
      return agg;
    }, {});
    const results = res.reduce((agg, e) => {
      agg[e.urn] = e.score;
      return agg;
    }, {});
    return Object.assign(template, results);
  },

  _handleError(urns, error) {
    const { errors, pending } = this.getProperties('errors', 'pending');

    const newError = urns;
    const newErrors = new Set([...errors, newError]);

    const newPending = new Set(pending);
    [...urns].forEach(urn => newPending.delete(urn));

    this.setProperties({ errors: newErrors, pending: newPending });
  }
});
