import { observer, computed, set, get } from '@ember/object';
import { later, debounce } from '@ember/runloop';
import { reads, gt, or } from '@ember/object/computed';
import { inject as service } from '@ember/service';
import Controller from '@ember/controller';
import {
  filterObject,
  filterPrefix,
  hasPrefix,
  toBaselineUrn,
  toBaselineRange,
  toCurrentUrn,
  toOffsetUrn,
  toFilters,
  toFilterMap,
  appendFilters,
  dateFormatFull,
  colorMapping
} from 'thirdeye-frontend/utils/rca-utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/shared/eventTableColumns';
import filterBarConfig from 'thirdeye-frontend/shared/filterBarConfig';
import moment from 'moment';
import config from 'thirdeye-frontend/config/environment';
import _ from 'lodash';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';

const PREVIEW_DATE_FORMAT = 'MMM DD, hh:mm a';

export default Controller.extend({
  detectionConfig: null,

  metricUrn: null,

  output: ['nothing here'],

  timeseries: null,

  baseline: null,

  analysisRange: [moment().subtract(1, 'month').valueOf(), moment().valueOf()],

  compareMode: 'wo1w',

  compareModeOptions: [
    'wo1w',
    'wo2w',
    'wo3w',
    'wo4w',
    'mean4w',
    'median4w',
    'min4w',
    'max4w',
    'none'
  ],

  errorTimeseries: null,

  errorBaseline: null,

  errorAnomalies: null,

  colorMapping: colorMapping,

  anomalies: computed('output', function () {
    return this._filterAnomalies(get(this, 'output'));
  }),

  anomaliesGrouped: computed('anomalies', function () {
    return this._groupByDimensions(get(this, 'anomalies'));
  }),

  anomaliesGroupedFormatted: computed('anomaliesGrouped', function () {
    const output = {};
    const anomaliesGrouped = get(this, 'anomaliesGrouped');
    Object.keys(anomaliesGrouped).forEach(key => {
      const anomalies = anomaliesGrouped[key];
      const outputKey = `${key} (${anomalies.length})`;
      let outputValue = anomalies.map(anomaly => this._formatAnomaly(anomaly)).sort();

      if (outputValue.length > 7) {
        outputValue = _.slice(outputValue, 0, 3).concat(['...'], _.slice(outputValue, -3));
      }

      output[outputKey] = outputValue;
    });
    return output;
  }),

  series: computed('anomalies', 'timeseries', 'baseline', function () {
    const metricUrn = get(this, 'metricUrn');
    const anomalies = get(this, 'anomalies');
    const anomaliesGrouped = get(this, 'anomaliesGrouped');
    const timeseries = get(this, 'timeseries');
    const baseline = get(this, 'baseline');

    const series = {};

    if (!_.isEmpty(anomaliesGrouped)) {
      const filters = toFilters(metricUrn);
      const key = this._makeKey(toFilterMap(filters));

      let anomaliesList = [];
      if (_.isEmpty(key)) {
        anomaliesList = anomalies;
      } else if (!_.isEmpty(anomaliesGrouped[key])) {
        anomaliesList = anomaliesGrouped[key];
      }

      anomaliesList.forEach(anomaly => {
        series[this._formatAnomaly(anomaly)] = {
          timestamps: [anomaly.startTime, anomaly.endTime],
          values: [1, 1],
          type: 'line',
          color: 'teal'
        }
      });
    }

    if (!_.isEmpty(timeseries)) {
      series['current'] = {
        timestamps: timeseries.timestamp,
        values: timeseries.value,
        type: 'line',
        color: 'blue'
      };
    }

    if (!_.isEmpty(baseline)) {
      series['baseline'] = {
        timestamps: baseline.timestamp,
        values: baseline.value,
        type: 'line',
        color: 'light-blue'
      };
    }

    return series;
  }),

  _makeKey(dimensions) {
    return Object.values(dimensions).join(', ')
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(PREVIEW_DATE_FORMAT)} (${this._makeKey(anomaly.dimensions)})`;
  },

  _filterAnomalies(rows) {
    return rows.filter(row => (row.startTime && row.endTime && !row.child));
  },

  _groupByDimensions(anomalies) {
    const grouping = {};
    anomalies.forEach(anomaly => {
      const key = this._makeKey(anomaly.dimensions);
      grouping[key] = (grouping[key] || []).concat([anomaly]);
    });
    return grouping;
  },

  _fetchTimeseries() {
    const metricUrn = get(this, 'metricUrn');
    const range = get(this, 'analysisRange');
    const granularity = '15_MINUTES';
    const timezone = moment.tz.guess();

    set(this, 'errorTimeseries', null);

    const urlCurrent = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=current&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlCurrent)
      .then(checkStatus)
      .then(res => set(this, 'timeseries', res))
      .catch(err => set(this, 'errorTimeseries', err));

    set(this, 'errorBaseline', null);

    const offset = get(this, 'compareMode');
    const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlBaseline)
      .then(checkStatus)
      .then(res => set(this, 'baseline', res))
      .catch(err => set(this, 'errorBaseline', err));
  },

  _fetchAnomalies() {
    const analysisRange = get(this, 'analysisRange');
    const url = `/detection/preview?start=${analysisRange[0]}&end=${analysisRange[1]}`;

    const jsonString = get(this, 'detectionConfig');

    set(this, 'errorAnomalies', null);

    fetch(url, { method: 'POST', body: jsonString })
      .then(checkStatus)
      .then(res => set(this, 'anomalies', this._filterAnomalies(res)))
      .catch(err => set(this, 'errorAnomalies', err));
  },

  actions: {
    onPreview() {
      set(this, 'output', ['loading ...']);

      this._fetchAnomalies();
    },

    onMetricChange(updates) {
      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');

      if (_.isEmpty(metricUrns)) { return; }

      const metricUrn = metricUrns[0];

      set(this, 'metricUrn', metricUrn);

      this._fetchTimeseries();
    },

    onCompareMode(compareMode) {
      set(this, 'compareMode', compareMode);

      this._fetchTimeseries();
    }
  }
});
