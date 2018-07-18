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

  detectionConfigName: null,

  datasetName: null,

  checkboxDefault: true,

  filterOptions: {},

  metrics: null,

  metricsProperties: null,

  detectionConfigCron: null,

  metricUrn: null,

  output: 'nothing here',

  anomalies: null,

  timeseries: null,

  baseline: null,

  filterMap: null,

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

  axis: {
    y: {
      show: true
    },
    y2: {
      show: false,
      min: 0,
      max: 1
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

  toggleChecked: false,

  anomaliesGrouped: computed('anomalies', function () {
    const anomalies = get(this, 'anomalies');
    if (_.isEmpty(anomalies)) {
      return {};
    }

    return this._groupByDimensions(anomalies);
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
          color: 'teal',
          axis: 'y2'
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
      .then(res => set(this, 'output', 'got timeseries'))
      .catch(err => set(this, 'errorTimeseries', err));

    set(this, 'errorBaseline', null);

    const offset = get(this, 'compareMode');
    const urlBaseline = `/rootcause/metric/timeseries?urn=${metricUrn}&start=${range[0]}&end=${range[1]}&offset=${offset}&granularity=${granularity}&timezone=${timezone}`;
    fetch(urlBaseline)
      .then(checkStatus)
      .then(res => set(this, 'baseline', res))
      .then(res => set(this, 'output', 'got baseline'))
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
      .then(res => set(this, 'output', 'got anomalies'))
      .catch(err => set(this, 'errorAnomalies', err));
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

    onCompareMode(compareMode) {
      set(this, 'output', 'fetching time series ...');

      set(this, 'compareMode', compareMode);

      this._fetchTimeseries();
    },


    onSave() {
      const url = `/dataset-auto-onboard/metrics?dataset=` + get(this, 'datasetName');
      fetch(url)
        .then(res => res.json())
        .then(res => this.set('metrics', res))
        .then(res => this.set('metricProperties',
          res.reduce(function (obj, metric) {
            obj[metric["name"]] = {
              "id": metric['id'],
              "urn": "thirdeye:metric:" + metric['id'],
              "monitor": true
            };
            return obj;
        }, {})))
        .then(res => this.set('metricUrn', res[Object.keys(res)[0]]['id']))
        .then(res => fetch(`/data/autocomplete/filters/metric/${res}`)
          .then(checkStatus)
          .then(res => this.setProperties({filterOptions: res}))
          .then(() => this.send('onSelect')))
        .catch(error => console.error(`Fetch Error =\n`, error));
      },

    toggleCheckBox(name) {
      const metricProperties = get(this, 'metricProperties');
      metricProperties[name]['monitor'] = !metricProperties[name]['monitor'];
    },

    onChangeValue(property, name, value) {
      const metricProperties = get(this, 'metricProperties');
      metricProperties[name][property] = value;
    },

    onFilters(metricName, filters) {
      const metricProperties = get(this, 'metricProperties');
      const filterMap = JSON.parse(filters);
      let metricUrn = "thirdeye:metric:" + metricProperties[metricName]['id'];
      Object.keys(filterMap).forEach(
        function (key) {
          filterMap[key].forEach(
            function (value) {
              metricUrn = metricUrn + ":" + key + "=" + value;
            }
          )
        }
      );
      metricProperties[metricName]['urn'] = metricUrn;
    },

    onSubmit() {
      const metricProperties = get(this, 'metricProperties');
      const datasetName = get(this, 'datasetName');
        Object.keys(metricProperties).forEach(
        function(key) {
          const properties = metricProperties[key];
          if (!properties['monitor']){
            return;
          }
          const detectionConfig = {
            name: datasetName + key + "detection",
            cron: "0 10/15 * * * ? *",
            lastTimestamp: 0,
            properties: {
              className: "com.linkedin.thirdeye.detection.algorithm.MergeWrapper",
              maxGap: 7200000,
              nested: [
                {
                  className: "com.linkedin.thirdeye.detection.algorithm.DimensionWrapper",
                  metricUrn: properties['urn'],
                  nested: [
                    {
                      className: "com.linkedin.thirdeye.detection.algorithm.MovingWindowAlgorithm",
                      baselineWeeks: 4,
                      windowSize: "4 weeks",
                      changeDuration: "7d",
                      outlierDuration: "12h",
                      aucMin: -10,
                      zscoreMin: -4,
                      zscoreMax: 4
                    }
                  ]
                }
              ]
            }
          };
          if (properties['topk']){
            detectionConfig['properties']['nested']['k'] = properties['topk'];
          }
          if (properties['minvalue']) {
            detectionConfig['properties']['nested']['minValue'] = properties['minvalue'];
          }
          if (properties['mincontribution']){
            detectionConfig['properties']['nested']['minContribution'] = properties['mincontribution'];
          }
          console.log(detectionConfig);
        }
      );
    }
  },
});
