import { observer, computed, set, get, getProperties } from '@ember/object';
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
  colorMapping,
  stripTail,
  extractTail,
  toColor
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

  detectionConfigName: null,

  detectionConfigCron: null,

  metricUrn: null,

  output: 'nothing here',

  anomalies: null,

  diagnostics: null,

  diagnosticsPath: null,

  diagnosticsValues: null,

  timeseries: null,

  baseline: null,

  analysisRange: [moment().subtract(2, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],

  displayRange: [moment().subtract(3, 'month').startOf('hour').valueOf(), moment().startOf('hour').valueOf()],

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

  axis: {
    y: {
      show: true
    },
    y2: {
      show: false
    },
    x: {
      type: 'timeseries',
      show: true,
      tick: {
        fit: false
      }
    }
  },

  zoom: {
    enabled: true,
    rescale: true
  },

  legend: {
    show: false
  },

  anomalyMetricUrns: computed('anomalies', function () {
    const anomalies = get(this, 'anomalies') || [];
    const metricUrns = new Set(anomalies.map(anomaly => stripTail(anomaly.metricUrn)));

    // TODO refactor this side-effect
    this._fetchEntities(metricUrns)
      .then(res => set(this, 'metricEntities', res));

    return metricUrns;
  }),

  metricEntities: null,

  anomalyMetricEntities: computed('anomalyMetricUrns', 'metricEntities', function () {
    const { anomalyMetricUrns, metricEntities } = getProperties(this, 'anomalyMetricUrns', 'metricEntities');
    if (_.isEmpty(anomalyMetricUrns) || _.isEmpty(metricEntities)) { return []; }
    return [...anomalyMetricUrns].filter(urn => urn in metricEntities).map(urn => metricEntities[urn]).sortBy('name');
  }),

  anomalyMetricUrnDimensions: computed('anomalies', function () {
    const anomalies = get(this, 'anomalies');
    const urn2dimensions = {};
    anomalies.forEach(anomaly => {
      const baseUrn = stripTail(anomaly.metricUrn);
      if (!_.isEqual(baseUrn, anomaly.metricUrn)) {
        urn2dimensions[baseUrn] = urn2dimensions[baseUrn] || new Set();
        urn2dimensions[baseUrn].add(anomaly.metricUrn);
      }
    });
    return urn2dimensions;
  }),

  anomalyMetricUrnDimensionLabels: computed('anomalies', function () {
    const anomalies = get(this, 'anomalies');
    const metricUrns = new Set(anomalies.map(anomaly => anomaly.metricUrn));

    const urn2count = {};
    [...anomalies].forEach(anomaly => {
      const urn = anomaly.metricUrn;
      urn2count[urn] = (urn2count[urn] || 0) + 1;
    });

    const urn2labels = {};
    [...metricUrns].forEach(urn => {
      const filters = toFilters(urn);
      urn2labels[urn] = filters.map(arr => arr[1]).join(", ") + ` (${urn2count[urn]})`;
    });
    return urn2labels;
  }),

  anomaliesByMetricUrn: computed('anomalies', function () {
    const anomalies = get(this, 'anomalies');
    const urn2anomalies = {};
    anomalies.forEach(anomaly => {
      const urn = anomaly.metricUrn;
      urn2anomalies[urn] = (urn2anomalies[urn] || []).concat([anomaly]);
    });
    return urn2anomalies;
  }),

  series: computed(
    'anomalies',
    'timeseries',
    'baseline',
    'diagnosticsSeries',
    'analysisRange',
    'displayRange',
    function () {
      const metricUrn = get(this, 'metricUrn');
      const anomalies = get(this, 'anomalies');
      const timeseries = get(this, 'timeseries');
      const baseline = get(this, 'baseline');
      const diagnosticsSeries = get(this, 'diagnosticsSeries');
      const analysisRange = get(this, 'analysisRange');
      const displayRange = get(this, 'displayRange');

      const series = {};

      if (!_.isEmpty(anomalies)) {

        anomalies
          .filter(anomaly => anomaly.metricUrn === metricUrn)
          .forEach(anomaly => {
            const key = this._formatAnomaly(anomaly);
            series[key] = {
              timestamps: [anomaly.startTime, anomaly.endTime],
              values: [1, 1],
              type: 'line',
              color: 'teal',
              axis: 'y2'
            };
            series[key + '-region'] = Object.assign({}, series[key], {
              type: 'region',
              color: 'orange'
            });
          });
      }

      if (timeseries && !_.isEmpty(timeseries.value)) {
        series['current'] = {
          timestamps: timeseries.timestamp,
          values: timeseries.value,
          type: 'line',
          color: toColor(metricUrn)
        };
      }

      if (baseline && !_.isEmpty(baseline.value)) {
        series['baseline'] = {
          timestamps: baseline.timestamp,
          values: baseline.value,
          type: 'line',
          color: 'light-' + toColor(metricUrn)
        };
      }

      // detection range
      if (timeseries && !_.isEmpty(timeseries.value)) {
        series['pre-detection-region'] = {
          timestamps: [displayRange[0], analysisRange[0]],
          values: [1, 1],
          type: 'region',
          color: 'grey'
        };
      }

      return Object.assign(series, diagnosticsSeries);
    }
  ),

  diagnosticsSeries: computed(
    'diagnostics',
    'diagnosticsPath',
    'diagnosticsValues',
    function () {
      const diagnosticsPath = get(this, 'diagnosticsPath');
      const diagnosticsValues = get(this, 'diagnosticsValues') || [];

      const series = {};

      diagnosticsValues.forEach(value => {
        const diagnostics = this._makeDiagnosticsSeries(diagnosticsPath, value);
        if (!_.isEmpty(diagnostics)) {
          series[`diagnostics-${value}`] = diagnostics;
        }
      });

      const changeSeries = this._makeDiagnosticsPoints(diagnosticsPath);
      Object.keys(changeSeries).forEach(key => {
        series[`diagnostics-${key}`] = changeSeries[key];
      });

      return series;
    }
  ),

  diagnosticsPathOptions: computed('diagnostics', function () {
    const diagnostics = get(this, 'diagnostics');
    return this._makePaths('', diagnostics);
  }),

  diagnosticsValueOptions: computed('diagnostics', 'diagnosticsPath', function () {
    const diagnosticsPath = get(this, 'diagnosticsPath');
    const diagnostics = get(this, 'diagnostics.' + diagnosticsPath);
    if (_.isEmpty(diagnostics)) { return []; }
    return Object.keys(diagnostics.data);
  }),

  _makePaths(prefix, diagnostics) {
    if (_.isEmpty(diagnostics)) { return []; }

    const directPaths = Object.keys(diagnostics)
      .filter(key => key.startsWith('thirdeye:metric:'))
      .map(key => prefix + `${key}`);

    const nestedPaths = Object.keys(diagnostics)
      .filter(key => !key.startsWith('thirdeye:metric:'))
      .map(key => this._makePaths(`${prefix}${key}.`, diagnostics[key]))
      .reduce((agg, paths) => agg.concat(paths), []);

    return directPaths.concat(nestedPaths);
  },

  _makeKey(dimensions) {
    return Object.values(dimensions).join(', ')
  },

  _formatAnomaly(anomaly) {
    return `${moment(anomaly.startTime).format(PREVIEW_DATE_FORMAT)} (${this._makeKey(anomaly.dimensions)})`;
  },

  _filterAnomalies(rows) {
    return rows.filter(row => (row.startTime && row.endTime && !row.child));
  },

  _makeDiagnosticsSeries(path, key) {
    try {
      const source = get(this, 'diagnostics.' + path + '.data');

      if (_.isEmpty(source.timestamp) || _.isEmpty(source[key])) { return; }

      return {
        timestamps: source.timestamp,
        values: source[key],
        type: 'line',
        axis: 'y2'
      }

    } catch (err) {
      return undefined;
    }
  },

  _makeDiagnosticsPoints(path) {
    try {
      const changepoints = get(this, 'diagnostics.' + path + '.changepoints');

      if (_.isEmpty(changepoints)) { return {}; }

      const out = {};
      changepoints.forEach((p, i) => {
        out[`changepoint-${i}`] = {
          timestamps: [p, p + 1],
          values: [1, 0],
          type: 'line',
          color: 'red',
          axis: 'y2'
        };

        out[`changepoint-${i}-region`] = {
          timestamps: [p, p + 3600000 * 24],
          values: [1, 1],
          type: 'region',
          color: 'red',
          axis: 'y2'
        };
      });

      return out;

    } catch (err) {
      return {};
    }
  },

  _fetchTimeseries() {
    const metricUrn = get(this, 'metricUrn');
    const range = get(this, 'displayRange');
    const granularity = '15_MINUTES';
    const timezone = moment.tz.guess();

    set(this, 'errorTimeseries', null);

    const urlCurrent = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=current&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlCurrent)
      .then(checkStatus)
      .then(res => set(this, 'timeseries', res))
      .then(res => set(this, 'output', 'got timeseries'))
      // .catch(err => set(this, 'errorTimeseries', err));

    set(this, 'errorBaseline', null);

    const offset = get(this, 'compareMode');
    const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlBaseline)
      .then(checkStatus)
      .then(res => set(this, 'baseline', res))
      .then(res => set(this, 'output', 'got baseline'))
      // .catch(err => set(this, 'errorBaseline', err));
  },

  _fetchAnomalies() {
    const analysisRange = get(this, 'analysisRange');
    const url = `/detection/preview?start=${analysisRange[0]}&end=${analysisRange[1]}&diagnostics=true`;

    const jsonString = get(this, 'detectionConfig');

    set(this, 'errorAnomalies', null);

    fetch(url, { method: 'POST', body: jsonString })
      .then(checkStatus)
      .then(res => {
        set(this, 'anomalies', this._filterAnomalies(res.anomalies));
        set(this, 'diagnostics', res.diagnostics);
      })
      .then(res => set(this, 'output', 'got anomalies'))
      // .catch(err => set(this, 'errorAnomalies', err));
  },

  _fetchEntities(urns) {
    const urnString = [...urns].join(',');
    const url = `/rootcause/raw?framework=identity&urns=${urnString}`;
    return fetch(url)
      .then(checkStatus)
      .then(res => res.reduce((agg, entity) => {
        agg[entity.urn] = entity;
        return agg;
      }, {}));
  },

  _writeDetectionConfig() {
    const detectionConfigBean = {
      name: get(this, 'detectionConfigName'),
      cron: get(this, 'detectionConfigCron'),
      properties: JSON.parse(get(this, 'detectionConfig')),
      lastTimestamp: 0
    };

    const jsonString = JSON.stringify(detectionConfigBean);

    return fetch(`/thirdeye/entity?entityType=DETECTION_CONFIG`, { method: 'POST', body: jsonString })
      .then(checkStatus)
      .then(res => set(this, 'output', `saved '${detectionConfigBean.name}' as id ${res}`))
      .catch(err => set(this, 'errorAnomalies', err));
  },

  actions: {
    onPreview() {
      set(this, 'output', 'loading anomalies ...');

      this._fetchAnomalies();
    },

    onMetricChange(updates) {
      set(this, 'output', 'fetching time series ...');

      const metricUrns = filterPrefix(Object.keys(updates), 'thirdeye:metric:');

      if (_.isEmpty(metricUrns)) { return; }

      const metricUrn = metricUrns[0];

      set(this, 'metricUrn', metricUrn);

      this._fetchTimeseries();
    },

    onMetricLink(metricUrn) {
      set(this, 'metricUrn', metricUrn);
      this._fetchTimeseries();

      // select matching diagnostics
      const diagnosticsPathOptions = get(this, 'diagnosticsPathOptions');
      const candidate = diagnosticsPathOptions.find(path => path.includes(metricUrn));
      if (!_.isEmpty(candidate)) {
        set(this, 'diagnosticsPath', candidate);
      }
    },

    onCompareMode(compareMode) {
      set(this, 'output', 'fetching time series ...');

      set(this, 'compareMode', compareMode);

      this._fetchTimeseries();
    },

    onDiagnosticsPath(diagnosticsPath) {
      set(this, 'diagnosticsPath', diagnosticsPath);
    },

    onDiagnosticsValues(diagnosticsValues) {
      set(this, 'diagnosticsValues', diagnosticsValues);
    },

    onSave() {
      set(this, 'output', 'saving detection config ...');

      this._writeDetectionConfig();
    }
  }
});
