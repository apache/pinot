import Service from '@ember/service';
import { inject as service } from '@ember/service';
import {
  trimTimeRanges,
  filterPrefix,
  toBaselineRange,
  toDimensionsUrn
} from 'thirdeye-frontend/utils/rca-utils';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import _ from 'lodash';

const ROOTCAUSE_CALLGRAPH_ENDPOINT = '/rootcause/query';
const ROOTCAUSE_CALLGRAPH_PRIORITY = 15;

export default Service.extend({
  edges: null, // {}

  context: null, // {}

  pending: null, // Set

  errors: null, // Set({ urn, error })

  fetcher: service('services/rootcause-fetcher'),

  init() {
    this._super(...arguments);
    this.setProperties({ edges: {}, context: {}, pending: new Set(), errors: new Set() });
  },

  clearErrors() {
    this.setProperties({ errors: new Set() });
  },

  request(requestContext, urns) {
    const { context } = this.getProperties('context');

    const metrics = [...urns].filter(urn => urn.startsWith('thirdeye:metric:'));

    if(_.isEqual(context, requestContext)) {
      return;
    }

    // new analysis range: evict all, reload
    this.get('fetcher').resetPrefix(ROOTCAUSE_CALLGRAPH_ENDPOINT);
    this.setProperties({ context: _.cloneDeep(requestContext), edges: {}, pending: new Set(metrics) });

    if (_.isEmpty(metrics)) {
      return;
    }

    // call graph properties
    const fetcher = this.get('fetcher');

    const dimensionsUrns = [...metrics].map(toDimensionsUrn);

    // TODO generalize replacement
    const dimensionsUrnsModified = dimensionsUrns
      .map(urn => urn.replace(/:data_center/, ':callee_fabric'))
      .map(urn => urn.replace(/:fabric/, ':callee_fabric'))
      .map(urn => urn.replace(/:service/, ':callee_container'))
      .map(urn => urn.replace(/:data_center/, ':callee_fabric'));

    const url = this._makeUrl('callgraph', requestContext, dimensionsUrnsModified);
    fetcher.fetch(url, ROOTCAUSE_CALLGRAPH_PRIORITY)
      .then(checkStatus)
      .then(res => this._complete(requestContext, res))
      .catch(error => this._handleError(urns, error));
  },

  _complete(requestContext, incoming) {
    const { context } = this.getProperties('context');

    if (!_.isEqual(context, requestContext)) {
      return;
    }

    const edges = incoming.reduce((agg, e) => { agg[e.urn] = e; return agg; }, {});

    this.setProperties({ edges, pending: new Set() });
  },

  _makeUrl(framework, context, urns) {
    const urnString = filterPrefix(urns, 'thirdeye:dimensions:').join(',');
    const ranges = trimTimeRanges(context.anomalyRange, context.analysisRange);

    const baselineRange = toBaselineRange(ranges.anomalyRange, context.compareMode);
    return `${ROOTCAUSE_CALLGRAPH_ENDPOINT}?framework=${framework}` +
      `&anomalyStart=${ranges.anomalyRange[0]}&anomalyEnd=${ranges.anomalyRange[1]}` +
      `&baselineStart=${baselineRange[0]}&baselineEnd=${baselineRange[1]}` +
      `&analysisStart=${ranges.analysisRange[0]}&analysisEnd=${ranges.analysisRange[1]}` +
      `&urns=${urnString}`;
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
