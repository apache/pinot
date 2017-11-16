import Ember from 'ember';
import { checkStatus, filterPrefix, toBaselineRange } from 'thirdeye-frontend/helpers/utils';
import fetch from 'fetch';
import _ from 'lodash';

export default Ember.Service.extend({
  entities: null, // {}

  context: null, // {}

  pending: null, // Set

  init() {
    this._super(...arguments);
    this.setProperties({ entities: {}, context: {}, pending: new Set() });
  },

  request(requestContext, urns) {
    console.log('rootcauseEntitiesCache: request()', requestContext, urns);
    const { context } = this.getProperties('context');
    if (_.isEqual(context, requestContext)) {
      console.log('rootcauseEntitiesCache: request: context is up-to-date. ignoring.');
      return;
    }

    const frameworks = new Set(['relatedEvents', 'relatedDimensions', 'relatedMetrics']);

    this.setProperties({ context: _.cloneDeep(requestContext), pending: frameworks });

    frameworks.forEach(framework => {
      const url = this._makeUrl(framework, requestContext);
      fetch(url)
        // .then(checkStatus) // TODO why doesn't this return parsed json here?
        .then(res => res.json())
        .then(this._jsonToEntities)
        .then(incoming => this._complete(requestContext, urns, incoming, framework));
    });
  },

  _complete(requestContext, pinnedUrns, incoming, framework) {
    console.log('rootcauseEntitiesCache: complete()', requestContext, pinnedUrns, incoming, framework);

    // only accept latest result
    const { context } = this.getProperties('context');
    if (!_.isEqual(context, requestContext)) {
      console.log('rootcauseEntitiesCache: _complete: received stale result. ignoring.');
      return;
    }

    // evict unselected
    const { entities, pending } = this.getProperties('entities', 'pending');
    const stale = new Set(this._evictionCandidates(entities, framework));
    const staleSelected = new Set([...stale].filter(urn => pinnedUrns.has(urn)));
    const staleUnselected = new Set([...stale].filter(urn => !pinnedUrns.has(urn)));

    // rebuild remaining cache
    const remaining = {};
    Object.keys(entities).filter(urn => !staleUnselected.has(urn)).forEach(urn => remaining[urn] = entities[urn]);
    Object.keys(entities).filter(urn => staleSelected.has(urn)).forEach(urn => remaining[urn].score = -1);

    // merge
    const newEntities = Object.assign({}, remaining, incoming);

    // update pending
    const newPending = new Set(pending);
    newPending.delete(framework);

    this.setProperties({ entities: newEntities, pending: newPending });
  },

  _evictionCandidates(entities, framework) {
    if (framework == 'relatedEvents') {
      return filterPrefix(Object.keys(entities), 'thirdeye:event:');
    }
    if (framework == 'relatedDimensions') {
      return filterPrefix(Object.keys(entities), 'thirdeye:dimension:');
    }
    if (framework == 'relatedMetrics') {
      return filterPrefix(Object.keys(entities), ['thirdeye:metric:', 'frontend:metric:']);
    }
  },

  _makeUrl(framework, context) {
    const urnString = [...context.urns].join(',');
    const baselineRange = toBaselineRange(context.anomalyRange, context.compareMode);
    return `/rootcause/query?framework=${framework}` +
      `&anomalyStart=${context.anomalyRange[0]}&anomalyEnd=${context.anomalyRange[1]}` +
      `&baselineStart=${baselineRange[0]}&baselineEnd=${baselineRange[1]}` +
      `&analysisStart=${context.analysisRange[0]}&analysisEnd=${context.analysisRange[1]}` +
      `&urns=${urnString}`;
  },

  _jsonToEntities(res) {
    if (_.isEmpty(res)) {
      return {};
    }
    return res.reduce((agg, e) => { agg[e.urn] = e; return agg; }, {});
  }
});
