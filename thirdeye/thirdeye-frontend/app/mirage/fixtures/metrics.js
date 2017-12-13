/**
 * Returns metric information about the first alert
 */
import alertConfig from 'thirdeye-frontend/mocks/alertConfig';
import anomalyFunction from 'thirdeye-frontend/mocks/anomalyFunction';

const id = alertConfig[0].reportConfigCollection.reportMetricConfigs.metricId;
const { collection: dataset, metric: name } = anomalyFunction[0];
const alias = `${dataset}::${name}`;

export default [
  {
    id,
    "version": 0,
    "createdBy": null,
    "updatedBy": null,
    name,
    dataset,
    alias,
    "datatype": "LONG",
    "derived": false,
    "derivedMetricExpression": null,
    "defaultAggFunction": "SUM",
    "rollupThreshold": 0.01,
    "inverseMetric": false,
    "cellSizeExpression": null,
    "active": true,
    "extSourceLinkInfo": null,
    "extSourceLinkTimeGranularity": null,
    "metricProperties": null,
    "datasetConfig": null
  }
];
