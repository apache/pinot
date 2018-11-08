/**
 * Custom model table component
 * Constructs the select box for the resolution
 * @module custom/anomalies-table/resolution
 *
 * @example for usage in models table columns definitions
 *   {
 *     propertyName: 'feedback',
 *     component: 'custom/anomalies-table/resolution',
 *     title: 'Resolution',
 *     className: 'anomalies-table__column',
 *     disableFiltering: true
 *   },
 */
import Component from "@ember/component";
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';
import { inject as service } from '@ember/service';
import {
  set,
  get,
  computed,
  setProperties,
  getWithDefault
} from '@ember/object';

export default Component.extend({
  tagName: '',//using tagless so i can add my own in hbs
  anomalyResponseNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
  anomalyDataUrl: getAnomalyDataUrl(),
  showResponseSaved: false,
  isUserReported: false,
  hasComment: false,
  renderStatusIcon: true,

  didReceiveAttrs() {
    this._super(...arguments);
    const anomalyComment = get(this.record.anomaly, 'comment');
    const hasComment = (anomalyComment && anomalyComment.replace(/ /g, '') !== 'null');
    const isUserReported = get(this.record.anomaly, 'source') === 'USER_LABELED_ANOMALY';
    setProperties(this, {
      hasComment,
      isUserReported
    });
  },

  actions: {
    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} humanizedAnomaly - the humanized anomaly entity
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     */
     onChangeAnomalyResponse: async function(humanizedAnomaly, selectedResponse, inputObj) {
      const responseObj = anomalyUtil.anomalyResponseObj.find(res => res.name === selectedResponse);
      set(inputObj, 'selected', selectedResponse);
      // Reset icon display props
      setProperties(this, {
        renderStatusIcon: false,
        showResponseFailed: false,
        showResponseSaved: false
      });
      try {
        const id = humanizedAnomaly.get('id');
        // Save anomaly feedback
        await anomalyUtil.updateAnomalyFeedback(id, responseObj.value);
        // We make a call to ensure our new response got saved
        const savedAnomaly = await anomalyUtil.verifyAnomalyFeedback(id);
        // TODO: right now we will update the union wrapper cached record for this anomaly
        humanizedAnomaly.set('anomaly.feedback', responseObj.value);
        const filterMap = getWithDefault(savedAnomaly, 'searchFilters.statusFilterMap', null);
        // This verifies that the status change got saved as key in the anomaly statusFilterMap property
        const keyPresent = filterMap && Object.keys(filterMap).find(key => responseObj.status.includes(key));
        if (keyPresent) {
          humanizedAnomaly.set('anomalyFeedback', selectedResponse);
          set(this, 'showResponseSaved', true);
        } else {
          throw 'Response not saved';
        }
      } catch (err) {
        setProperties(this, {
          showResponseFailed: true,
          showResponseSaved: false
        });
      }
      set(this, 'renderStatusIcon', true);
    }
  }
});
