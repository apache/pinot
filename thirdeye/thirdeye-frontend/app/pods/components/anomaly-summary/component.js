/**
 * Displays summary for each anomaly in anomalies route
 * @module components/anomaly-summary
 * @exports anomaly-summary
 */
import Component from '@ember/component';
import {
  set,
  get,
  computed,
  getProperties
} from '@ember/object';
import { colorMapping, toColor, makeTime } from 'thirdeye-frontend/utils/rca-utils';
import { getFormattedDuration,
  anomalyResponseMapNew,
  verifyAnomalyFeedback,
  anomalyResponseObj,
  anomalyResponseObjNew,
  updateAnomalyFeedback
} from 'thirdeye-frontend/utils/anomaly';
import RSVP from "rsvp";
import fetch from 'fetch';
import { checkStatus, humanizeFloat } from 'thirdeye-frontend/utils/utils';
import columns from 'thirdeye-frontend/shared/anomaliesTableColumns';
import moment from 'moment';
import _ from 'lodash';

const TABLE_DATE_FORMAT = 'MMM DD, hh:mm A'; // format for anomaly table

export default Component.extend({
  /**
   * Overrides ember-models-table's css classes
   */
  classes: {
    table: 'table table-striped table-bordered table-condensed'
  },

  columns: columns,
  /**
   * Anomaly Id, passed from parent
   */
  anomalyId: null,
  /**
   * Anomaly data, fetched using the anomalyId
   */
  anomalyData: {},
  /**
   * Anomaly data, fetched using the anomalyId
   */
  current: null,
  /**
   * Anomaly data, fetched using the anomalyId
   */
  predicted: null,
  /**
   * List of associated classes
   */
  colorMapping: colorMapping,
  zoom: {
    enabled: false,
    rescale: true
  },

  legend: {
    show: true,
    position: 'right'
  },
  isLoading: false,
  feedbackOptions: ['Not reviewed yet', 'Yes - unexpected', 'Expected temporary change', 'Expected permanent change', 'No change observed'],
  labelMap: anomalyResponseMapNew,
  labelResponse: {},

  init() {
    this._super(...arguments);
    this._fetchAnomalyData();
  },

  axis: computed(
    'anomalyData',
    function () {
      const anomalyData = get(this, 'anomalyData');

      return {
        y: {
          show: true,
          tick: {
            format: function(d){return humanizeFloat(d);}
          }
        },
        y2: {
          show: false,
          min: 0,
          max: 1
        },
        x: {
          type: 'timeseries',
          show: true,
          min: anomalyData.startTime,
          max: anomalyData.endTime,
          tick: {
            fit: false,
            format: (d) => {
              const t = makeTime(d);
              if (t.valueOf() === t.clone().startOf('day').valueOf()) {
                return t.format('MMM D (ddd)');
              }
              return t.format('h:mm a');
            }
          }
        }
      };
    }
  ),

  series: computed(
    'anomalyData',
    'current',
    'predicted',
    function () {
      const {
        anomalyData, current, predicted
      } = getProperties(this, 'anomalyData', 'current', 'predicted');

      const series = {};

      if (!_.isEmpty(anomalyData)) {
        const key = this._formatAnomaly(anomalyData);
        series[key] = {
          timestamps: [anomalyData.startTime, anomalyData.endTime],
          values: [1, 1],
          type: 'region',
          color: 'orange'
        };
      }

      if (current && !_.isEmpty(current.value)) {
        series['current'] = {
          timestamps: current.timestamp,
          values: current.value,
          type: 'line',
          color: 'blue'
        };
      }

      if (predicted && !_.isEmpty(predicted.value)) {
        series['predicted'] = {
          timestamps: predicted.timestamp,
          values: predicted.value,
          type: 'line',
          color: 'orange'
        };
      }
      return series;
    }
  ),

  /**
   * formats anomaly for table
   * @method anomaly
   * @return {Object}
   */
  anomaly: computed(
    'anomalyData',
    'labelResponse',
    function() {
      const anomalyData = get(this, 'anomalyData');
      const labelResponse = get(this, 'labelResponse');
      let tableAnomaly = {};

      if (anomalyData) {
        const a = anomalyData; //for convenience below
        const change = (a.avgBaselineVal !== 0 && a.avgBaselineVal !== "Infinity" && a.avgCurrentVal !== "Infinity") ? (a.avgCurrentVal/a.avgBaselineVal - 1.0) * 100.0 : 0;
        tableAnomaly = {
          anomalyId: a.id,
          metricUrn: a.metricUrn,
          start: a.startTime,
          end: a.endTime,
          metricName: a.metric,
          dataset: a.collection,
          startDateStr: this._formatAnomaly(a),
          durationStr: getFormattedDuration(a.startTime, a.endTime),
          shownCurrent: a.avgCurrentVal === "Infinity" ? 0 : humanizeFloat(a.avgCurrentVal),
          shownBaseline: a.avgBaselineVal === "Infinity" ? 0 : humanizeFloat(a.avgBaselineVal),
          change: change,
          shownChangeRate: humanizeFloat(change),
          anomalyFeedback: a.feedback ? a.feedback.feedbackType : "NONE",
          showResponseSaved: (labelResponse.anomalyId === a.id) ? labelResponse.showResponseSaved : false,
          showResponseFailed: (labelResponse.anomalyId === a.id) ? labelResponse.showResponseFailed: false
        };
      }
      return tableAnomaly;
    }
  ),

  _fetchAnomalyData() {
    const anomalyId = get(this, 'anomalyId');
    const anomalyUrl = `/dashboard/anomalies/view/${anomalyId}`;

    set(this, 'isLoading', true);

    fetch(anomalyUrl)
      .then(checkStatus)
      .then(res => {
        set(this, 'anomalyData', res);
        const timeZone = 'America/Los_Angeles';
        const currentUrl = `/rootcause/metric/timeseries?urn=${res.metricUrn}&start=${res.startTime}&end=${res.endTime}&offset=current&timezone=${timeZone}`;
        const predictedUrl = `/detection/predicted-baseline/${anomalyId}?start=${res.startTime}&end=${res.endTime}`;
        const timeseriesHash = {
          current: fetch(currentUrl).then(res => checkStatus(res, 'get', true)),
          predicted: fetch(predictedUrl).then(res => checkStatus(res, 'get', true))
        };
        return RSVP.hash(timeseriesHash);
      })
      .then((res) => {
        set(this, 'current', res.current);
        set(this, 'predicted', res.predicted);
        set(this, 'isLoading', false);
      })
      .catch(err => {
        set(this, 'isLoading', false);
      });
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(TABLE_DATE_FORMAT)}`;
  },

  actions: {
    /**
     * Handle dynamically saving anomaly feedback responses
     * @method onChangeAnomalyResponse
     * @param {Object} anomalyRecord - the anomaly being responded to
     * @param {String} selectedResponse - user-selected anomaly feedback option
     * @param {Object} inputObj - the selection object
     */
    onChangeAnomalyFeedback: async function(anomalyRecord, selectedResponse) {
      const anomalyData = get(this, 'anomalyData');
      // Reset status icon
      set(this, 'renderStatusIcon', false);
      const responseObj = anomalyResponseObj.find(res => res.name === selectedResponse);
      // get the response object from anomalyResponseObjNew
      const newFeedbackValue = anomalyResponseObjNew.find(res => res.name === selectedResponse).value;
      try {
        // Save anomaly feedback
        await updateAnomalyFeedback(anomalyRecord.anomalyId, responseObj.value);
        // We make a call to ensure our new response got saved
        const anomaly = await verifyAnomalyFeedback(anomalyRecord.anomalyId);

        if (anomaly.feedback && responseObj.value === anomaly.feedback.feedbackType) {
          this.set('labelResponse', {
            anomalyId: anomalyRecord.anomalyId,
            showResponseSaved: true,
            showResponseFailed: false
          });

          // replace anomaly feedback with selectedFeedback
          anomalyData.feedback = {
            feedbackType: newFeedbackValue
          };

          set(this, 'anomalyData', anomalyData);
        } else {
          throw 'Response not saved';
        }
      } catch (err) {
        this.set('labelResponse', {
          anomalyId: anomalyRecord.anomalyId,
          showResponseSaved: false,
          showResponseFailed: true
        });
      }
      // Force status icon to refresh
      set(this, 'renderStatusIcon', true);
    }
  }
});
