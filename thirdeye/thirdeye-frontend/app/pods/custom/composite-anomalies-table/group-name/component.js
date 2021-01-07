import Component from '@ember/component';
import pubSub from 'thirdeye-frontend/utils/pub-sub';

export default Component.extend({
  actions: {
    drilldownGroupConstituent(anomalyId) {
      pubSub.publish('onAnomalyDrilldown', anomalyId);
    }
  }
});
