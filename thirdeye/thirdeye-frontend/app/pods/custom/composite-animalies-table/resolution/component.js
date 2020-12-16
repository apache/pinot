import Component from '@ember/component';
import { set } from '@ember/object';

export default Component.extend({
  didReceiveAttrs() {
    this._super(...arguments);
  },
  actions: {
    onChangeAnomalyResponse: (anomalyObject, selection, options) => {
      set(options, 'selected', selection); // set selected option to user's selection
      set(anomalyObject.feedback, 'selected', selection); // set anomalies feedback to user's selection
      /* TODO: add API call to update anomalies on the BackEnd */
    }
  }
});
