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
  appendFilters,
  dateFormatFull
} from 'thirdeye-frontend/utils/rca-utils';
import EVENT_TABLE_COLUMNS from 'thirdeye-frontend/shared/eventTableColumns';
import filterBarConfig from 'thirdeye-frontend/shared/filterBarConfig';
import moment from 'moment';
import config from 'thirdeye-frontend/config/environment';
import _ from 'lodash';
import { checkStatus } from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';

const PREVIEW_DATE_FORMAT = 'YYYY-MM-HH hh:mm';

export default Controller.extend({
  detectionConfig: null,

  metricUrn: null,

  output: ['nothing here'],

  timeseries: null,

  analysisRange: [moment().subtract(1, 'month').valueOf(), moment().valueOf()],

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
      output[key] = anomalies.map(anomaly => this._formatAnomaly(anomaly)).sort();
    });
    return output;
  }),

  series: computed('anomalies', 'timeseries', function () {
    const timeseries = get(this, 'timeseries');

    if (_.isEmpty(timeseries)) { return; }

    return {
      myseries: {
        timestamps: timeseries.timestamp,
        values: timeseries.value,
        type: 'line',
        color: 'blue'
      }
    };
  }),

  _formatDimensions(dimensions) {
    return Object.values(dimensions).sort().join(', ');
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(PREVIEW_DATE_FORMAT)} to ` +
           `${moment(anomaly.endTime).format(PREVIEW_DATE_FORMAT)}`;
  },

  _filterAnomalies(rows) {
    return rows.filter(row => (row.startTime && row.endTime && !row.child));
  },

  _groupByDimensions(anomalies) {
    const grouping = {};
    anomalies.forEach(anomaly => {
      const key = Object.values(anomaly.dimensions).join(', ');
      grouping[key] = (grouping[key] || []).concat([anomaly]);
    });
    return grouping;
  },

  _fetchTimeseries() {
    const metricUrn = get(this, 'metricUrn');
    const range = get(this, 'analysisRange');
    const offset = 'current';
    const granularity = '15_MINUTES';
    const timezone = moment.tz.guess();

    const url = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    return fetch(url)
      .then(checkStatus)
      .then(res => set(this, 'timeseries', res))
      .catch(err => set(this, 'output', [err]));
  },

  _fetchAnomalies() {
    const analysisRange = get(this, 'analysisRange');
    const url = `/detection/preview?start=${analysisRange[0]}&end=${analysisRange[1]}`;

    const jsonString = get(this, 'detectionConfig');

    fetch(url, { method: 'POST', body: jsonString })
      .then(checkStatus)
      .then(res => set(this, 'output', res))
      .catch(err => set(this, 'output', [err]));
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
    }
  }
});
