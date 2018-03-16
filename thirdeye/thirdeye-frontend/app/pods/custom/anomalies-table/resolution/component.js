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
import { getWithDefault } from '@ember/object';
import * as anomalyUtil from 'thirdeye-frontend/utils/anomaly';
import { set, setProperties } from '@ember/object';
import { getAnomalyDataUrl } from 'thirdeye-frontend/utils/api/anomaly';

export default Component.extend({
  tagName: '',//using tagless so i can add my own in hbs
  anomalyResponseNames: anomalyUtil.anomalyResponseObj.mapBy('name'),
  anomalyDataUrl: getAnomalyDataUrl(),

  actions: {
    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} anomalyRecord - the anomaly being responded to
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     */
     onChangeAnomalyResponse: async function(anomalyRecord, selectedResponse, inputObj) {
      const responseObj = anomalyUtil.anomalyResponseObj.find(res => res.name === selectedResponse);
      set(inputObj, 'selected', selectedResponse);
      let res;
      try {
        // Save anomaly feedback
        res = await anomalyUtil.updateAnomalyFeedback(anomalyRecord.anomalyId, responseObj.value)
        // We make a call to ensure our new response got saved
        res = await anomalyUtil.verifyAnomalyFeedback(anomalyRecord.anomalyId, responseObj.status)
        const filterMap = getWithDefault(res, 'searchFilters.statusFilterMap', null);
        if (filterMap && filterMap.hasOwnProperty(responseObj.status)) {
          setProperties(anomalyRecord, {
            anomalyFeedback: selectedResponse,
            showResponseSaved: true
          });
        } else {
          return Promise.reject(new Error('Response not saved'));
        }
      } catch (err) {
        setProperties(anomalyRecord, {
          showResponseFailed: true,
          showResponseSaved: false
        });
      }
    }
  }
});
