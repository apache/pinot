import Ember from 'ember';

const anomalyRange = [1509044400000, 1509422400000];
const baselineRange = [1508439600000, 1508817600000];
const analysisRange = [1508785200000, 1509422400000];
const urns = new Set(['thirdeye:metric:194591', 'thirdeye:dimension:countryCode:in:provided']);
const granularity = '30_MINUTES';

export default Ember.Route.extend({
  setupController: function (controller, model) {
    this._super(...arguments);
    console.log('route: setupController()');

    // TODO get initial attributes from query params
    controller.setProperties({
      selectedUrns: new Set([
        'thirdeye:metric:194591', 'frontend:baseline:metric:194591',
        'thirdeye:metric:194592', 'frontend:baseline:metric:194592',
        'thirdeye:event:holiday:2712391']),
      invisibleUrns: new Set(),
      hoverUrns: new Set(),
      filteredUrns: new Set(),
      context: { urns, anomalyRange, baselineRange, analysisRange, granularity },
    });
  }
});
