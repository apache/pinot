import {observer, computed, set, get} from '@ember/object';
import Controller from '@ember/controller';
import {checkStatus} from 'thirdeye-frontend/utils/utils';
import fetch from 'fetch';

export default Controller.extend({
  detectionConfigId: null,

  detectionConfigName: null,

  datasetName: null,

  datasets: null,

  dimensions: null,

  selectedMetric: null,

  filterOptions: null,

  metrics: null,

  metricsProperties: null,

  metricUrn: null,

  filterMap: null,

  toggleChecked: false,

  output: null,

  topk: null,

  minContribution: null,

  minValue: null,

  idToNames: null,

  selectedDimensions: '{}',

  selectedFilters: '{}',

  queryParams: ['detectionId'],

  generalFieldsEnabled: computed.or('dimensions'),

  metricsFieldEnabled: computed.or('metrics'),

  // TODO: replace with ember data
  hasDetectionId: observer('detectionId', async function () {
    const detectionId = this.get('detectionId');
    const res = await fetch(`/dataset-auto-onboard/` + detectionId).then(checkStatus);
    const nestedProperties = res['properties']['nested'];
    const nestedProperty = nestedProperties[0];

    // fill in values:
    this.setProperties({
      detectionConfigId: res['id'],
      detectionConfigName: res['name'],
      topk: nestedProperty['k'],
      minValue: nestedProperty['minValue'],
      minContribution: nestedProperty['minContribution'],
      datasetName: res['properties']['datasetName']
    });

    await this._datasetNameChanged();

    let dimensionBreakdownUrn = null;
    const idToNames = this.get('idToNames');
    const metricsProperties = get(this, 'metricsProperties');

    // fill in dimensions
    this.set('selectedDimensions', JSON.stringify({
      'dimensions': nestedProperty['dimensions']
    }));

    // fill in filters
    let urnPieces = nestedProperty['metricUrn'].split(':');
    const filters = {};
    let i;
    for (i = 3; i < urnPieces.length; i++) {
      const filter = urnPieces[i].split('=');
      if (filter[0] in filters) {
        filters[filter[0]].push(filter[1]);
      } else {
        filters[filter[0]] = [filter[1]];
      }
    }
    this.set('selectedFilters', JSON.stringify(filters));
    this._updateFilters();

    // fill in selected metrics
    const metricIds = nestedProperties.reduce(function (obj, property) {
      let urn;
      if ('nestedMetricUrn' in property) {
        urn = property['nestedMetricUrn'];
        dimensionBreakdownUrn = property['metricUrn'];
      } else {
        urn = property['metricUrn'];
      }
      obj.push(urn.split(':')[2]);
      return obj;
    }, []);

    Object.keys(metricsProperties).forEach(function (key) {
      if (metricIds.indexOf(metricsProperties[key]['id'].toString()) == -1) {
        set(metricsProperties[key], 'monitor', false);
      }
    });

    // fill in dimension breakdown metric
    this.set('selectedMetric', idToNames[this._metricUrnToId(dimensionBreakdownUrn)]);
  }),

  // TODO: replace with ember data
  _writeDetectionConfig(detectionConfigBean) {
    const jsonString = JSON.stringify(detectionConfigBean);
    return fetch(`/thirdeye/entity?entityType=DETECTION_CONFIG`, {method: 'POST', body: jsonString})
      .then(checkStatus)
      .then(res => set(this, 'output', `saved '${detectionConfigBean.name}' as id ${res}`))
      .catch(err => set(this, 'output', err));
  },

  _metricUrnToId(metricUrn) {
    return metricUrn.split(':')[2];
  },

  _updateFilters() {
    const filters = this.get('selectedFilters');
    const metricsProperties = get(this, 'metricsProperties');
    const filterMap = JSON.parse(filters);
    Object.keys(metricsProperties).forEach(function (key) {
      const metricProperty = metricsProperties[key];
      let metricUrn = "thirdeye:metric:" + metricProperty['id'];
      Object.keys(filterMap).forEach(function (key) {
        filterMap[key].forEach(function (value) {
          metricUrn = metricUrn + ":" + key + "=" + value;
        });
      });
      metricsProperties[key]['urn'] = metricUrn;
    });
  },

  // TODO: replace with ember data
  async _datasetNameChanged() {
    const url = `/dataset-auto-onboard/metrics?dataset=` + get(this, 'datasetName');
    const res = await fetch(url).then(checkStatus);
    const metricsProperties = res.reduce(function (obj, metric) {
      obj[metric["name"]] = {
        "id": metric['id'], "urn": "thirdeye:metric:" + metric['id'], "monitor": true
      };
      return obj;
    }, {});
    const metricUrn = metricsProperties[Object.keys(metricsProperties)[0]]['id'];
    const idToNames = {};
    Object.keys(metricsProperties).forEach(function (key) {
      idToNames[metricsProperties[key]['id']] = key;
    });

    this.setProperties({
      metricsProperties: metricsProperties,
      metrics: Object.keys(metricsProperties),
      idToNames: idToNames,
      metricUrn: metricUrn
    });

    const result = await fetch(`/data/autocomplete/filters/metric/${metricUrn}`).then(checkStatus);

    this.setProperties({
      filterOptions: result, dimensions: {dimensions: Object.keys(result)}
    });
  },

  actions: {
    onSave(dataset) {
      this.set('datasetName', dataset);
      this._datasetNameChanged();
    },

    toggleCheckBox(name) {
      const metricsProperties = get(this, 'metricsProperties');
      set(metricsProperties[name], 'monitor', !metricsProperties[name]['monitor']);
    },

    onChangeValue(property, value) {
      this.set(property, value);
    },

    onFilters(filters) {
      this.set('selectedFilters', filters);
      this._updateFilters();
    },

    onSubmit() {
      const metricsProperties = get(this, 'metricsProperties');
      const nestedProperties = [];
      const selectedMetric = this.get('selectedMetric');
      const detectionConfigId = this.get('detectionConfigId');
      const selectedDimensions = JSON.parse(this.get('selectedDimensions'));
      const topk = this.get('topk');
      const minValue = this.get('minValue');
      const minContribution = this.get('minContribution');
      Object.keys(metricsProperties).forEach(function (key) {
        const properties = metricsProperties[key];
        if (!properties['monitor']) {
          return;
        }
        const detectionConfig = {
          className: 'com.linkedin.thirdeye.detection.algorithm.DimensionWrapper', nested: [{
            className: 'com.linkedin.thirdeye.detection.algorithm.MovingWindowAlgorithm',
            baselineWeeks: 4,
            windowSize: '4 weeks',
            changeDuration: '7d',
            outlierDuration: '12h',
            aucMin: -10,
            zscoreMin: -4,
            zscoreMax: 4
          }]
        };
        if (selectedMetric == null) {
          detectionConfig['metricUrn'] = properties['urn'];
        } else {
          detectionConfig['metricUrn'] = metricsProperties[selectedMetric]['urn'];
          detectionConfig['nestedMetricUrn'] = properties['urn'];
        }
        if (selectedDimensions != null) {
          detectionConfig['dimensions'] = selectedDimensions['dimensions'];
        }
        if (topk != null) {
          detectionConfig['k'] = parseInt(topk);
        }
        if (minValue != null) {
          detectionConfig['minValue'] = parseFloat(minValue);
        }
        if (minContribution != null) {
          detectionConfig['minContribution'] = parseFloat(minContribution);
        }
        nestedProperties.push(detectionConfig);
      });

      const configResult = {
        "cron": "45 10/15 * * * ? *", "name": get(this, 'detectionConfigName'), "lastTimestamp": 0, "properties": {
          "className": "com.linkedin.thirdeye.detection.algorithm.MergeWrapper",
          "maxGap": 7200000,
          "nested": nestedProperties,
          "datasetName": get(this, 'datasetName')
        }
      };

      if (detectionConfigId != null) {
        configResult['id'] = detectionConfigId;
      }
      this._writeDetectionConfig(configResult);
    },

    onSelectDimension(dims) {
      this.set('selectedDimensions', dims);
    },

    onChangeName(name) {
      this.set('detectionConfigName', name);
    },

    onSelectMetric(name) {
      this.set('selectedMetric', name);
    },

    async onLoadDatasets() {
      const url = `/thirdeye/entity/DATASET_CONFIG`;
      const res = await fetch(url).then(checkStatus);
      const datasets = res.reduce(function (obj, datasetConfig) {
        obj.push(datasetConfig['dataset']);
        return obj;
      }, []);
      this.set('datasets', datasets);
    }
  }
});
