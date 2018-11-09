// Share constant file
export const deleteProps = {
  method: 'delete',
  headers: { 'content-type': 'Application/Json' },
  credentials: 'include'
};

export default {
  deleteProps
};


export const yamlAlertProps = `name: alert_name
metric: metric_name
dataset: dataset_name
pipelineType: Composite
dimensionExploration:
  dimensions:
    - continent
    - browserName
  minContribution: 0.05
filters:
  continent:
     - Europe
     - Asia
  os_name:
     - android
  browserName:
     - chrome
     - safari

anomalyDetection:
- detection:
    - type: BASELINE
      change: 0.5
  filter:
    - type: BUSINESS_RULE_FILTER
      siteWideImpactThreshold: 0.2

- detection:
    - type: THRESHOLD
  filter:
    - type: THRESHOLD_RULE_FILTER`;
