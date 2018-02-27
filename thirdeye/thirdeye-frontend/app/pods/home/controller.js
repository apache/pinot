import Controller from '@ember/controller';
import { computed, get } from '@ember/object';

export default Controller.extend({

  // applicationAnomalies: computed(
  //   'model',
  //   function() {
  //     const anomalies = get(this, 'model');
  //     anomalies.forEach(anomaly => {
  //       const { current, baseline } = anomaly;
  //       const value = {
  //         current,
  //         baseline
  //       };

  //       anomaly.value = value;
  //       anomaly.investigationLink = `/rootcause?anomalyId=${anomaly.id}`;
  //     });

  //     return anomalies;
  //   }
  // )

});
