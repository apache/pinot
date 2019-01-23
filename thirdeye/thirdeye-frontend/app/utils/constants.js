// Share constant file
export const deleteProps = {
  method: 'delete',
  headers: { 'content-type': 'Application/Json' },
  credentials: 'include'
};

export default {
  deleteProps
};


export const yamlAlertProps = `# Below are all dummy example. Please update accordingly.
# give a name for this detection
detectionName: name_of_the_detection
# the metric to detect the anomalies
metric: metric_name
# the data set name for this metric
dataset: dataset_name
# ThirdEye pipeline type. Just fill in Composite
pipelineType: Composite

# (Optional) Config dimension exploration
dimensionExploration:
  # Create an alert for each dimension value in the dimension
  dimensions:
    - dimensionName

  # (Optional) only create alert for the dimension value if the contribution to
  # overall metric is above the threshold
  minContribution: 0.05

  # (Optional) only create alert for the top k dimension values
  k: 10

# (Optional) Filter the metric by
filters:
  dimensionName1:
     - dimensionValue1
     - dimensionValue2
  dimensionName2:
     - dimensionValue3

# configure rules of anomaly detection
rules:
# configure the first rule
- # configure the detection rule. ThirdEye will detect anomalies based on the
  # detection rules.
  detection:
      # give a name for the detection rule
    - name: detection_rule_1
      # ThirdEye rule type
      type: PERCENTAGE_RULE
      # parameters for this rule
      params:
        offset: wo1w
        change: 0.1

  # (Optional) configure the exclusion rule. (Exclude the anomalies you don't
  # want to see but detected by the detection rule above)
  filter:
    - name: filter_rule_1
      type: ABSOLUTE_CHANGE_FILTER
      params:
        threshold: 10000

# configure more rule if you'd like to
- detection:
    - name: detection_rule_2
      type: ABSOLUTE_CHANGE_RULE
      params:
        offset: wo1w
        change: 1000000
        pattern: UP

`;

export const yamIt = function(metric, dataset){
  return `# Below are all dummy example. Please update accordingly.
# give a name for this detection
detectionName: name_of_the_detection
# the metric to detect the anomalies
metric: ${metric}
# the data set name for this metric
dataset: ${dataset}
# ThirdEye pipeline type. Just fill in Composite
pipelineType: Composite

# (Optional) Config dimension exploration
dimensionExploration:
  # Create an alert for each dimension value in the dimension
  dimensions:
    - dimensionName

  # (Optional) only create alert for the dimension value if the contribution to
  # overall metric is above the threshold
  minContribution: 0.05

  # (Optional) only create alert for the top k dimension values
  k: 10

# (Optional) Filter the metric by
filters:
  dimensionName1:
     - dimensionValue1
     - dimensionValue2
  dimensionName2:
     - dimensionValue3

# configure rules of anomaly detection
rules:
# configure the first rule
- # configure the detection rule. ThirdEye will detect anomalies based on the
  # detection rules.
  detection:
      # give a name for the detection rule
    - name: detection_rule_1
      # ThirdEye rule type
      type: PERCENTAGE_RULE
      # parameters for this rule
      params:
        offset: wo1w
        change: 0.1

  # (Optional) configure the exclusion rule. (Exclude the anomalies you don't
  # want to see but detected by the detection rule above)
  filter:
    - name: filter_rule_1
      type: ABSOLUTE_CHANGE_FILTER
      params:
        threshold: 10000

# configure more rule if you'd like to
- detection:
    - name: detection_rule_2
      type: ABSOLUTE_CHANGE_RULE
      params:
        offset: wo1w
        change: 1000000
        pattern: UP

`;
};

export const yamlAlertSettings = `# Below are all dummy example. Please update accordingly.
subscriptionGroupName: test_subscription_group
application: thirdeye-internal
subscribedDetections:
  - name_of_the_detection_above

alertSchemes:
- type: EMAIL
recipients:
 to:
  - "me@company.com"
 cc:
  - "cc_email@company.com"
fromAddress: thirdeye-dev@linkedin.com

cron: "0 0/5 * 1/1 * ? *"
type: DEFAULT_ALERTER_PIPELINE
active: true
referenceLinks:
 "My Company FAQs": "http://www.company.com/faq"`;
