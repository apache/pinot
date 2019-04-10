// Share constant file
export const deleteProps = {
  method: 'delete',
  headers: { 'content-type': 'Application/Json' },
  credentials: 'include'
};

export const yamlAlertProps = `# Below is a sample template for setting up a WoW percentage rule. You may refer the documentation for more examples and update the fields accordingly.

# Give a name for this anomaly detection pipeline (should be unique)
detectionName: name_of_the_detection

# Tell the alert recipients what it means if this alert is fired
description: If this alert fires then it means so-and-so and check so-and-so for irregularities

# The metric you want to do anomaly detection on. You may type a few characters and look ahead (ctrl + space) to auto-fill
metric: metric_name

# The dataset or UMP table name to which the metric belongs. Look ahead should auto populate this field
dataset: dataset_name

# ThirdEye pipeline type. Only "Composite" is supported now.
pipelineType: Composite

# Configure multiple rules. ThirdEye supports single or a list of rules combined together with "OR" relationship
rules:
- detection:                      # Eg. Detect anomalies if the week over week change of this metric is more than 10%
    - name: detection_rule_1      # Give a unique name for this detection rule.
      type: PERCENTAGE_RULE       # Configure the detection type here. See doc for more details.
      params:                     # The parameters for this rule. Different rules have different params.
        offset: wo1w              # Compare current value with last week. (Values supported - wo1w, wo2w, median3w etc)
        percentageChange: 0.1     # The threshold above which you want to be alerted.
        pattern: UP_OR_DOWN       # Alert when value goes up or down by the configured threshold. (Values supported - UP, DOWN, UP_OR_DOWN)

# Refer to examples and documentation for exploring other types of detection rules, algorithms and filters with more detailed settings.
`;

export const yamlAlertSettings = `# Below is a sample subscription group template. You may refer the documentation and update accordingly.

# The name of the subscription group. You may choose an existing or a provide a new subscription group name
subscriptionGroupName: test_subscription_group

# Every alert in ThirdEye is attached to an application. Please specify the registered application name here. You may request for a new application by dropping an email to ask_thirdeye
application: thirdeye-internal

# The default notification type. See additional settings for details and exploring other notification types like dimension alerter.
type: DEFAULT_ALERTER_PIPELINE

# List of detection names that you want to subscribe. Copy-paste the detection name from the above anomaly detection config here.
subscribedDetections:
  - name_of_the_detection_above

# Configure how you want to be alerted. You can receive the standard ThirdEye email alert (recommended)
# or for advanced critical use-cases setup Iris alert by referring to the documentation
alertSchemes:
- type: EMAIL
recipients:
 to:
  - "me@company.com"          # Specify alert recipient email address here
  - "me@company.com"
 cc:
  - "cc_email@company.com"
fromAddress: thirdeye-dev@linkedin.com

# The frequency at which you want to be notified. Typically you want to be notified immediately after
# an anomaly is detected. The below cron runs every 5 minutes. Use online cronmaker to compute this.
cron: "0 0/5 * 1/1 * ? *"

# Enable or disable notification of alert
active: true

# The below links will appear in the email alerts. This will help alert recipients to quickly refer and act on.
referenceLinks:
  "Oncall Runbook": "http://go/oncall"
  "Thirdeye FAQs": "http://go/thirdeyefaqs"

`;

export const toastOptions = {
  timeOut: 10000
};

export default {
  deleteProps,
  yamlAlertProps,
  yamlAlertSettings,
  toastOptions
};
