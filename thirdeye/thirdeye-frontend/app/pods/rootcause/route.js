import Ember from 'ember';
import RSVP from 'rsvp';
import fetch from 'fetch';

const primaryMetricUrn = 'thirdeye:metric:194591';
const anomalyRange = [1509044400000, 1509422400000];
const baselineRange = [1508439600000, 1508817600000];
const analysisRange = [1508785200000, 1509422400000];
const contextUrns = ['thirdeye:metric:194591', 'thirdeye:dimension:countryCode:in:provided'];

export default Ember.Route.extend({
  model() {
    // TODO make search context dynamic
    // TODO move this to controller for async loading

    const eventUrl = makeFrameworkUrl('relatedEvents', anomalyRange, baselineRange, analysisRange, contextUrns);
    const dimensionUrl = makeFrameworkUrl('relatedDimensions', anomalyRange, baselineRange, analysisRange, contextUrns);
    const metricUrl = makeFrameworkUrl('relatedMetrics', anomalyRange, baselineRange, analysisRange, contextUrns);

    const eventEntities = fetch(eventUrl).then(e => e.json());
    const dimensionEntities = fetch(dimensionUrl).then(e => e.json());
    const metricEntities = fetch(metricUrl).then(e => e.json());

    return RSVP.hash({
      eventEntities, dimensionEntities, metricEntities
    }).then(res => Object.assign(res, { primaryMetricUrn, anomalyRange, baselineRange, analysisRange, contextUrns }));
  },

  afterModel(model) {
    console.log('model', model);
  },

  setupController: function (controller, model) {
    this._super(...arguments);
    controller.setProperties({
      selectedUrns: new Set(['thirdeye:metric:194592', 'thirdeye:event:holiday:2712391']),
      invisibleUrns: new Set(),
      filteredUrns: new Set(),
      hoverUrns: new Set(),
      anomalyRange: anomalyRange,
      baselineRange: baselineRange,
      analysisRange: analysisRange,
    });
  }
});

const makeFrameworkUrl = (framework, anomaly, baseline, analysis, urns) => {
  const urnString = urns.join(',');
  return `/rootcause/query?framework=${framework}&anomalyStart=${anomaly[0]}&anomalyEnd=${anomaly[1]}&baselineStart=${baseline[0]}&baselineEnd=${baseline[1]}&analysisStart=${analysis[0]}&analysisEnd=${analysis[1]}&urns=${urnString}`;
}
