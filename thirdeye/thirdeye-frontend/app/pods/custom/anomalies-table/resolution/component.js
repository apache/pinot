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
import { getWithDefault, computed, get, set } from '@ember/object';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { setProperties } from '@ember/object';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';
import { inject as service } from '@ember/service';

export default Component.extend({
  tagName: '',//using tagless so i can add my own in hbs
  anomalyResponseNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
  anomalyDataUrl: getAnomalyDataUrl(),
  showResponseSaved: false,
  isUserReported: false,
  hasComment: false,

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
      let res;
      try {
        const id = humanizedAnomaly.get('id');
        // Save anomaly feedback
        res = await anomalyUtil.updateAnomalyFeedback(id, responseObj.value);
        // We make a call to ensure our new response got saved
        res = await anomalyUtil.verifyAnomalyFeedback(id, responseObj.status);
        // TODO: right now we will update the union wrapper cached record for this anomaly
        humanizedAnomaly.set('anomaly.feedback', responseObj.value);

        const filterMap = getWithDefault(res, 'searchFilters.statusFilterMap', null);
        if (filterMap && filterMap.hasOwnProperty(responseObj.status)) {
          humanizedAnomaly.set('anomalyFeedback', selectedResponse);
          set(this, 'showResponseSaved', true);
        } else {
          return Promise.reject(new Error('Response not saved'));
        }
      } catch (err) {
        setProperties(this, {
          showResponseFailed: true,
          showResponseSaved: false
        });
      }
    }
  }
});
