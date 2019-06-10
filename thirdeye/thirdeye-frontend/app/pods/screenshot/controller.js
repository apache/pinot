import { colorMapping, makeTime } from 'thirdeye-frontend/utils/rca-utils';
import {
  get,
  computed,
  getProperties
} from '@ember/object';
import Controller from '@ember/controller';
import { humanizeFloat, stripNonFiniteValues } from 'thirdeye-frontend/utils/utils';
import moment from 'moment';
import _ from 'lodash';

const TABLE_DATE_FORMAT = 'MMM DD, hh:mm A'; // format for anomaly table and legend

export default Controller.extend({
  /**
   * Anomaly data, fetched using the anomalyId
   */
  anomalyData: {},
  /**
   * current time series
   */
  current: null,
  /**
   * predicted time series
   */
  predicted: null,
  /**
   * imported color mapping for graph
   */
  colorMapping: colorMapping,

  zoom: {
    enabled: false,
    rescale: true
  },

  // legend and point are for the graph
  legend: {
    show: true,
    position: 'right'
  },

  point: {
    show: false
  },

  anomaly: computed(
    'anomalyData',
    function() {
      return !_.isEmpty(get(this, 'anomalyData'));
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
        const key = 'Anomaly';
        series[key] = {
          timestamps: [anomalyData.startTime, anomalyData.endTime],
          values: [1, 1],
          type: 'region',
          color: 'screenshot-anomaly'
        };
      }

      if (current && !_.isEmpty(current.current)) {
        series['Current'] = {
          timestamps: current.timestamp,
          values: current.current,
          type: 'line',
          color: 'screenshot-current'
        };
      }

      if (predicted && !_.isEmpty(predicted.value)) {
        series['Predicted'] = {
          timestamps: predicted.timestamp,
          values: predicted.value,
          type: 'line',
          color: 'screenshot-predicted'
        };
      }

      if (predicted && !_.isEmpty(predicted.upper_bound)) {
        series['Upper and lower bound'] = {
          timestamps: predicted.timestamp,
          values: stripNonFiniteValues(predicted.upper_bound),
          type: 'line',
          color: 'screenshot-bounds'
        };
      }

      if (predicted && !_.isEmpty(predicted.lower_bound)) {
        series['lowerBound'] = {
          timestamps: predicted.timestamp,
          values: stripNonFiniteValues(predicted.lower_bound),
          type: 'line',
          color: 'screenshot-bounds'
        };
      }

      return series;
    }
  ),

  axis: computed(
    'anomalyData',
    'series',
    function () {
      const {
        anomalyData,
        series
      } = this.getProperties('anomalyData', 'series');

      let start = anomalyData.startTime;
      let end = anomalyData.endTime;
      if (series.Current && series.Current.timestamps && Array.isArray(series.Current.timestamps)) {
        start = series.Current.timestamps[0];
        end = series.Current.timestamps[series.Current.timestamps.length - 1];
      }

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
          min: start,
          max: end,
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

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(TABLE_DATE_FORMAT)}`;
  }
});
