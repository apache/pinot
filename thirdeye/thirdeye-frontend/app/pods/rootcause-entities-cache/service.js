import Ember from 'ember';
import { filterObject, filterPrefix, toBaselineRange, toColor } from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';
import _ from 'lodash';

const ROOTCAUSE_ANALYSIS_DURATION_MAX = 1209600000; // 14 days (in millis)
const ROOTCAUSE_ANOMALY_DURATION_MAX = 604800000; // 7 days (in millis)

export default Ember.Service.extend({
  entities: null, // {}

  context: null, // {}

  nativeUrns: null, // Set

  pending: null, // Set

  errors: null, // Set({ framework, error })

  init() {
    this._super(...arguments);
    this.setProperties({ entities: {}, context: {}, pending: new Set(), nativeUrns: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context, entities, nativeUrns } = this.getProperties('context', 'entities', 'nativeUrns');

    // special case: urn identity
    const requestNativeUrns = new Set(filterPrefix(urns, ['thirdeye:metric:', 'thirdeye:event:anomaly:']));
    if (!_.isEqual(nativeUrns, requestNativeUrns)) {
      this.setProperties({ nativeUrns: requestNativeUrns });

      const missingSelectedEntities = [...requestNativeUrns].filter(urn => !entities[urn]);
      if (missingSelectedEntities) {
        fetch(this._makeIdentityUrl(requestNativeUrns))
          .then(checkStatus)
          .then(this._jsonToEntities)
          .then(incoming => this._complete(requestContext, urns, incoming, 'identity'))
          .catch(error => this._handleError('identity', error));
      }
    }

    // rootcause search
    if (!_.isEqual(context, requestContext)) {
      const newEntities = filterObject(entities, (e) => urns.has(e.urn));

      if (!requestContext.urns || !requestContext.urns.size) {
        this.setProperties({ context: _.cloneDeep(requestContext), entities: newEntities });
        return;
      }

      const frameworks = new Set(['events', 'metricAnalysis']);

      this.setProperties({ context: _.cloneDeep(requestContext), entities: newEntities, pending: frameworks });

      frameworks.forEach(framework => {
        fetch(this._makeUrl(framework, requestContext))
          .then(checkStatus)
          .then(this._jsonToEntities)
          .then(incoming => this._complete(requestContext, urns, incoming, framework))
          .catch(error => this._handleError(framework, error));
      });
    }
  },

  _complete(requestContext, pinnedUrns, incoming, framework) {
    // only accept latest result
    const { context } = this.getProperties('context');
    if (!_.isEqual(context, requestContext)) {
      // console.log('rootcauseEntitiesCache: _complete: received stale result. ignoring.');
      return;
    }

    const pinnedBaseUrns = new Set([...pinnedUrns]);

    // evict unselected
    const { entities, pending } = this.getProperties('entities', 'pending');
    const stale = new Set(this._evictionCandidates(entities, framework));
    const staleSelected = new Set([...stale].filter(urn => pinnedBaseUrns.has(urn)));
    const staleUnselected = new Set([...stale].filter(urn => !pinnedBaseUrns.has(urn)));

    // TODO dedicated _complete_identity() method?
    // adjust incoming scores for identity
    if (framework === 'identity') {
      Object.keys(incoming).filter(urn => urn in entities).forEach(urn => incoming[urn].score = entities[urn].score);
    }

    // augment color property
    Object.keys(incoming).forEach(urn => incoming[urn].color = toColor(urn));

    // rebuild remaining cache
    const remaining = {};
    Object.keys(entities).filter(urn => !staleUnselected.has(urn)).forEach(urn => remaining[urn] = entities[urn]);

    // merge
    const newEntities = Object.assign({}, remaining, incoming);

    // update pending
    const newPending = new Set(pending);
    newPending.delete(framework);

    this.setProperties({ entities: newEntities, pending: newPending });
  },

  _trimRanges(anomalyRange, analysisRange) {
    // trim anomaly range from start of anomaly range forward
    const newAnomalyDuration = Math.min(anomalyRange[1] - anomalyRange[0], ROOTCAUSE_ANOMALY_DURATION_MAX);
    const newAnomalyRange = [anomalyRange[0], anomalyRange[0] + newAnomalyDuration];

    // trim analysis range from end of anomaly range backward
    const newAnalysisDuration = Math.min(analysisRange[1] - analysisRange[0], ROOTCAUSE_ANALYSIS_DURATION_MAX);
    const newAnalysisRangeStart = Math.max(analysisRange[0], anomalyRange[1] - newAnalysisDuration);
    const newAnalysisRange = [newAnalysisRangeStart, anomalyRange[1]];

    return Object.assign({}, {
      anomalyRange: newAnomalyRange,
      analysisRange: newAnalysisRange
    });
  },

  _evictionCandidates(entities, framework) {
    switch (framework) {
      case 'events':
        return filterPrefix(Object.keys(entities), 'thirdeye:event:');
      case 'metricAnalysis':
        return filterPrefix(Object.keys(entities), 'thirdeye:metric:');
      case 'identity':
        return [];
      default:
        return [];
    }
  },

  _makeUrl(framework, context) {
    const urnString = filterPrefix(context.urns, 'thirdeye:metric:').map(encodeURIComponent).join(',');
    const ranges = this._trimRanges(context.anomalyRange, context.analysisRange);

    const baselineRange = toBaselineRange(ranges.anomalyRange, context.compareMode);
    return `/rootcause/query?framework=${framework}` +
      `&anomalyStart=${ranges.anomalyRange[0]}&anomalyEnd=${ranges.anomalyRange[1]}` +
      `&baselineStart=${baselineRange[0]}&baselineEnd=${baselineRange[1]}` +
      `&analysisStart=${ranges.analysisRange[0]}&analysisEnd=${ranges.analysisRange[1]}` +
      `&urns=${urnString}`;
  },

  _makeIdentityUrl(urns) {
    const urnString = [...urns].map(encodeURIComponent).join(',');
    return `/rootcause/raw?framework=identity&urns=${urnString}`;
  },

  _jsonToEntities(res) {
    if (_.isEmpty(res)) {
      return {};
    }
    return res.reduce((agg, e) => { agg[e.urn] = e; return agg; }, {});
  },

  _handleError(framework, error) {
    const { errors, pending } = this.getProperties('errors', 'pending');

    const newError = framework;
    const newErrors = new Set([...errors, newError]);

    const newPending = new Set(pending);
    newPending.delete(framework);

    this.setProperties({ errors: newErrors, pending: newPending });
  }
});
