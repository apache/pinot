import {observer, computed, set, get} from '@ember/object';
import {later, debounce} from '@ember/runloop';
import {reads, gt, or} from '@ember/object/computed';
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
import {checkStatus} from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';


export default Controller.extend({
  detectionConfig: null,

  detectionConfigName: null,

  datasetName: null,

  dimensions: null,

  selectedDimension: null,

  checkboxDefault: true,

  filterOptions: {},

  metrics: computed('metricsProperties', function () {
    return Object.keys(get(this, 'metricsProperties'));
  }),

  metricsProperties: {},

  detectionConfigCron: null,

  metricUrn: null,

  baseline: null,

  filterMap: null,

  toggleChecked: false,

  generalFieldsEnabled: computed.or('dimensions'),

  _writeDetectionConfig() {
    const detectionConfigBean = {
      name: get(this, 'detectionConfigName'),
      cron: get(this, 'detectionConfigCron'),
      properties: JSON.parse(get(this, 'detectionConfig')),
      lastTimestamp: 0
    };

    const jsonString = JSON.stringify(detectionConfigBean);

    return fetch(`/thirdeye/entity?entityType=DETECTION_CONFIG`, {method: 'POST', body: jsonString})
      .then(checkStatus)
      .then(res => set(this, 'output', `saved '${detectionConfigBean.name}' as id ${res}`))
      .catch(err => set(this, 'errorAnomalies', err));
  },

  actions: {
    onSave() {
      const url = `/dataset-auto-onboard/metrics?dataset=` + get(this, 'datasetName');
      fetch(url)
        .then(res => res.json())
        .then(res => this.set('metricProperties', res.reduce(function (obj, metric) {
          obj[metric["name"]] = {
            "id": metric['id'], "urn": "thirdeye:metric:" + metric['id'], "monitor": true
          };
          return obj;
        }, {})))
        .then(res => this.set('metricUrn', res[Object.keys(res)[0]]['id']))
        .then(res => fetch(`/data/autocomplete/filters/metric/${res}`)
          .then(checkStatus)
          .then(res => {
            this.set('filterOptions', res);
            this.set('dimensions', Object.keys(res));
          }))
        .catch(error => console.error(`Fetch Error =\n`, error));
    },

    toggleCheckBox(name) {
      const metricProperties = get(this, 'metricProperties');
      metricProperties[name]['monitor'] = !metricProperties[name]['monitor'];
    },

    onChangeValue(property, value) {
      const metricProperties = get(this, 'metricProperties');
      Object.keys(metricProperties).forEach(
        function (key) {
          metricProperties[key][property] = value;
        }
      );
    },

    onFilters(filters) {
      const metricProperties = get(this, 'metricProperties');
      const filterMap = JSON.parse(filters);
      Object.keys(metricProperties).forEach(function (key) {
        const metricProperty = metricProperties[key];
        let metricUrn = "thirdeye:metric:" + metricProperty['id'];
        Object.keys(filterMap).forEach(function (key) {
          filterMap[key].forEach(function (value) {
            metricUrn = metricUrn + ":" + key + "=" + value;
          })
        });
        metricProperties[key]['urn'] = metricUrn;
      });
    },

    onSubmit() {
      const metricProperties = get(this, 'metricProperties');
      const nestedProperties = [];
      Object.keys(metricProperties).forEach(function (key) {
        const properties = metricProperties[key];
        if (!properties['monitor']) {
          return;
        }
        const detectionConfig = {
              className: "com.linkedin.thirdeye.detection.algorithm.DimensionWrapper",
              metricUrn: properties['urn'],
              nested: [{
                className: "com.linkedin.thirdeye.detection.algorithm.MovingWindowAlgorithm",
                baselineWeeks: 4,
                windowSize: "4 weeks",
                changeDuration: "7d",
                outlierDuration: "12h",
                aucMin: -10,
                zscoreMin: -4,
                zscoreMax: 4
              }]
        };
        if (properties['topk']) {
          detectionConfig['k'] = properties['topk'];
        }
        if (properties['minvalue']) {
          detectionConfig['minValue'] = properties['minvalue'];
        }
        if (properties['mincontribution']) {
          detectionConfig['minContribution'] = properties['mincontribution'];
        }
        nestedProperties.push(detectionConfig);
      });

      const configResult = {
        "cron": "45 10/15 * * * ? *",
        "name": get(this, 'detectionConfigName'),
        "lastTimestamp": 0,
        "properties": {
          "className": "com.linkedin.thirdeye.detection.algorithm.MergeWrapper",
          "maxGap": 7200000,
          "nested": nestedProperties
        }
      };

      console.log(configResult);
    },

    onSelectDimension(dim) {
      this.set('selectedDimension', dim);
    },

    onChangeName(name) {
      this.set('detectionConfigName', name);
    },

    onSelectMetric(name) {
      Object.keys(get(this, 'metricProperties')).forEach(function (key) {

      });
    }
  }
});
