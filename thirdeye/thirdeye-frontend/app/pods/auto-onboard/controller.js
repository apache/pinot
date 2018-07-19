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

  selectedDimensions: null,

  selectedMetric: null,

  checkboxDefault: true,

  filterOptions: {},

  metrics: null,

  metricsProperties: {},

  detectionConfigCron: null,

  metricUrn: null,

  baseline: null,

  filterMap: null,

  toggleChecked: false,

  output: null,

  generalFieldsEnabled: computed.or('dimensions'),

  metricsFieldEnabled: computed.or('metrics'),

  _writeDetectionConfig(detectionConfigBean) {
    const jsonString = JSON.stringify(detectionConfigBean);

    return fetch(`/thirdeye/entity?entityType=DETECTION_CONFIG`, {method: 'POST', body: jsonString})
      .then(checkStatus)
      .then(res => set(this, 'output', `saved '${detectionConfigBean.name}' as id ${res}`))
      .catch(err => set(this, 'output', err));
  },

  actions: {
    onSave() {
      const url = `/dataset-auto-onboard/metrics?dataset=` + get(this, 'datasetName');
      fetch(url)
        .then(res => res.json())
        .then(res => this.set('metrics', res))
        .then(res => this.set('metricsProperties', res.reduce(function (obj, metric) {
          obj[metric["name"]] = {
            "id": metric['id'], "urn": "thirdeye:metric:" + metric['id'], "monitor": true
          };
          return obj;
        }, {})))
        .then(res => {
          this.set('metrics', Object.keys(res));
          const metricUrn = res[Object.keys(res)[0]]['id'];
          this.set('metricUrn', res[Object.keys(res)[0]]['id']);
          fetch(`/data/autocomplete/filters/metric/${metricUrn}`)
            .then(checkStatus)
            .then(res => {
              this.set('filterOptions', res);
              this.set('dimensions', {dimensions: Object.keys(res)});
            })
        })
        .catch(error => console.error(`Fetch Error =\n`, error));
    },

    toggleCheckBox(name) {
      const metricsProperties = get(this, 'metricsProperties');
      metricsProperties[name]['monitor'] = !metricsProperties[name]['monitor'];
    },

    onChangeValue(property, value) {
      const metricsProperties = get(this, 'metricsProperties');
      Object.keys(metricsProperties).forEach(
        function (key) {
          metricsProperties[key][property] = value;
        }
      );
    },

    onFilters(filters) {
      const metricsProperties = get(this, 'metricsProperties');
      const filterMap = JSON.parse(filters);
      Object.keys(metricsProperties).forEach(function (key) {
        const metricProperty = metricsProperties[key];
        let metricUrn = "thirdeye:metric:" + metricProperty['id'];
        Object.keys(filterMap).forEach(function (key) {
          filterMap[key].forEach(function (value) {
            metricUrn = metricUrn + ":" + key + "=" + value;
          })
        });
        metricsProperties[key]['urn'] = metricUrn;
      });
    },

    onSubmit() {
      const metricsProperties = get(this, 'metricsProperties');
      const nestedProperties = [];
      const selectedMetric = this.get('selectedMetric');
      const selectedDimensions = this.get('selectedDimensions');
      Object.keys(metricsProperties).forEach(function (key) {
        const properties = metricsProperties[key];
        if (!properties['monitor']) {
          return;
        }
        const detectionConfig = {
              className: "com.linkedin.thirdeye.detection.algorithm.DimensionWrapper",
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
        if(selectedMetric == null) {
          detectionConfig['metricUrn'] = properties['urn'];
        } else {
          detectionConfig['metricUrn'] = metricsProperties[selectedMetric]['urn'];
          detectionConfig['nestedMetricUrn'] = properties['urn'];
        }

        if (selectedDimensions != null){
          debugger;
          detectionConfig['dimensions'] = selectedDimensions;
        }
        if (properties['topk']) {
          detectionConfig['k'] = parseInt(properties['topk']);
        }
        if (properties['minvalue']) {
          detectionConfig['minValue'] = parseFloat(properties['minvalue']);
        }
        if (properties['mincontribution']) {
          detectionConfig['minContribution'] = parseFloat(properties['mincontribution']);
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

      this._writeDetectionConfig(configResult);
    },

    onSelectDimension(dims) {
      const dimsMap = JSON.parse(dims);
      this.set('selectedDimensions', dimsMap['dimensions']);
    },

    onChangeName(name) {
      this.set('detectionConfigName', name);
    },

    onSelectMetric(name) {
      this.set('selectedMetric', name);
    }
  }
});
