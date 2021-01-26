import Component from '@ember/component';
import { set, getWithDefault, setProperties } from '@ember/object';

import { anomalyResponseObj, updateAnomalyFeedback, verifyAnomalyFeedback } from 'thirdeye-frontend/utils/anomaly';
import pubSub from 'thirdeye-frontend/utils/pub-sub';

export default Component.extend({
  renderStatusIcon: true,
  showResponseFailed: false,
  showResponseSaved: false,
  cascadeSelection: false,

  async submitFeedback() {
    const {
      feedback: { selected: selection },
      anomalyId
    } = this.record;

    try {
      const feedbackType = anomalyResponseObj.find((f) => f.name === selection).value;
      await updateAnomalyFeedback(anomalyId, feedbackType, this.cascadeSelection);

      // We make a call to ensure our new response got saved
      const savedAnomaly = await verifyAnomalyFeedback(anomalyId);
      const filterMap = getWithDefault(savedAnomaly, 'feedback.feedbackType', null);
      // This verifies that the status change got saved
      const keyPresent = filterMap && filterMap === feedbackType;

      if (keyPresent) {
        set(this, 'showResponseSaved', true);
      } else {
        throw 'Response not saved';
      }

      pubSub.publish('onFeedback', { anomalyId, feedbackType, cascade: this.cascadeSelection });
    } catch (err) {
      setProperties(this, {
        showResponseFailed: true,
        showResponseSaved: false
      });
    }
  },

  actions: {
    onChangeAnomalyResponse: async function ({ feedback, isLeaf = false }, selection) {
      set(feedback, 'selected', selection);

      if (isLeaf) {
        this.submitFeedback();
      }
    },

    onSelectCascade(selection) {
      this.set('cascadeSelection', selection);
    },

    onSubmit: async function () {
      this.submitFeedback();
    }
  }
});
