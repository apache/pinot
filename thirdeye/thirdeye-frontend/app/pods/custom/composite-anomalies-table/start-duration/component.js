import Component from '@ember/component';
import pubSub from 'thirdeye-frontend/utils/pub-sub';

export default Component.extend({
  actions: {
    drilldownAnomaly(anomalyId) {
      pubSub.publish('onAnomalyDrilldown', anomalyId);
    }
  }
});
