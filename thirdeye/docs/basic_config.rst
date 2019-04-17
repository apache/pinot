..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

Basic Alert Setup
==============================

Prerequisites
-------------

1. Before setting up an alert, you need to make sure you have the
data/metrics in ThirdEye.

- Pinot metrics are loaded. See :ref:`pinot`

- MySQL or Presto metrics are imported. See :ref:`import-sql-metric`.

2. Before running detection (i.e. `./run-backend.sh`), you need to have production MySQL database setup, since the demo database does not support 
simultaneous connections from front end and back end. See :ref:`production`.

Setting up an Alert in ThirdEye
-------------------------------

Setting up an alert in ThirdEye is a **2 step process**. It involves
writing two configuration files on the ThirdEye UI. These configurations
empower you to fully control the anomaly detection and unlocks the
flexibility to fine tune the platform to meet your needs.

The goal of these two configurations are

1. Setting up the  **detection** configuration - for  **how to detect anomalies**.

2. Setting up the  **subscription** configuration - for  **how you want to be subscribed or notified**.

We use YAML as the configuration file format. YAML is an
indentation-based markup language which aims to be both easy to read and
easy to write. You may refer a quick YAML
tutorial  `here <https://gettaurus.org/docs/YAMLTutorial/>`__. 

Step 1: Login to ThirdEye and click on the** *Create Alert* option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Go to  `go/thirdeye <https://go.corp.linkedin.com/thirdeye>`__ and
login. Click on "Create Alert" tab you see at the right top corner of
the page. This should land you on the page where you can now setup the
ThirdEye alerts. As we discussed above, there are 2 configurations
(detection and subscription) you would need to fill in.

Step 2: Writing the Detection Configuration (Define anomaly detection in YAML)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This configuration defines what kind of rules and detection functions
you want to run on the metrics. You may edit and tweak this
configuration even after setting up the alerts.

You can take the help of the template in the UI and the below sub-steps
to complete this configuration.

If you just want to play around, you may look at an existing sample
configuration file here
:ref:`templates-detection`,
edit them accordingly and move on to the next step (Step 3).


a. Provide the basic information in the detection configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Basic information starts with the name of detection pipeline,
description, metric name, dataset name and pipeline type. All these
fields are mandatory.

The description of these properties is shown as comments below:

.. code-block:: yaml

   # Give a unique name for this anomaly detection pipeline.
   detectionName: name_of_the_detection

   # Tell the alert recipients what it means if this alert is fired
   description: If this alert fires then it means so-and-so and check so-and-so for irregularities

   # The metric you want to do anomaly detection on.
   metric: metric_name

   # The data set name for the metric.
   dataset: dataset_name

The metric and dataset could be auto-completed (Ctrl + Space) by yaml
editor. For metrics from UMP/Pinot, the dataset name is the actual Pinot
table name. It has the naming convention of
<ump_dataset_name>**_additive** or
<ump_dataset_name>**_non_additive**, depends on whether the metric
is additive or not. For metrics from inGraph the dataset name is the
inGraph dashboard name.

b. Start adding detection rules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Detection rules tell ThirdEye what kind of algorithms or rules it needs
to run to detect anomalies. ThirdEye supports multiple rules combined
together with "OR" relationship.

For example, the config below defines a rule saying I would like to
generate anomalies if the week over week change of this metric is more
than 10%.

.. code-block:: yaml

   - detection:

      - name: detection_rule_1 # Unique name for this detection rule

      type: PERCENTAGE_RULE # The type for this detection rule or filter rule. See all supported detection rules and filter rules below.

      params: # The parameters for this rule. Different rules have different params.
         offset: wo1w
         percentageChange: 0.1

To see the complete list of detection rules, `click
here :ref:`all-detection-rules`.

To explore more advanced detection configuration settings, `click
here :ref:`advanced-detection`

c. A Complete detection configuration example with basic settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To see more examples,  `click
here. :ref:`templates-detection`.

.. code-block:: yaml

   # Provide a unique detection name
   detectionName: thirdEyeTeam_thirdeyeWebapp_pinotExceptionCounter_UP

   # Update the description
   description: If this alert fires then it means that we see are seeing
   lot of pinot call exceptions. Please check the controller logs to
   investigate.

   # Update and choose a different metric (Ctrl + Space to look ahead)

   metric: thirdeye_controller_pinotExceptionCounter

   dataset: thirdeye-all

   # Configure multiple rules. ThirdEye supports single or a list of rules combined together with "OR" relationship

   rules:
   - detection: # Eg. Detect anomalies if the week over week change of this metric is more than 10%
      - name: detection_rule_1 # Give a unique name for this detection rule.
      type: PERCENTAGE_RULE
      params:
         offset: wo1w # Compares current value with last week. (Values supported - wo1w, wo2w, median3w etc)
         percentageChange: 0.1 # The threshold above which you want to be alerted.
         pattern: UP # Alert when value goes up or down by the configured threshold. (Values supported - UP, DOWN, UP_OR_DOWN)

Step 3: (Beta) Preview the alert configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Click the preview drop down and then click the preview button.


ThirdEye will run anomaly detection for the period of last week using
your configuration and show you the anomalies result in the UI. If you
think the result is good you can go ahead to the next step. Otherwise,
you can go back and edit the detection YAML configuration and preview
again.

Step 4: Writing the Subscription-Group Configuration (Define notification settings)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This configuration defines who and how you want to be notified of the
anomalies. This configuration can be edited later as per your needs.

If you want to add the above detection rule to an existing subscription
group then,

1. Select your subscription group from the drop down which says *"Add
   this alert to an existing subscription group"*

2. Specify the *detectionName* you defined above under the
   *subscribedDetections* field in your subscription config.

Otherwise, skip this leaving *"Create a subscription group"* in the drop
down. Take the help of the template in the UI and the below sub-steps to
complete this configuration.

If you just want to play around, you may look at an existing sample
configuration file here
:ref:`templates-subscription`.
edit them accordingly and move on to the next step (Step 5).

a. Provide the basic information 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Basic information starts with the name of subscription group, the
registered application name and the subscription type. All these fields
are mandatory.

You can find the description of the properties in-line below:

.. code-block:: yaml

   # The name of the subscription group. You may choose an existing or a new subscription group name
   subscriptionGroupName: name_of_the_subscription_group

   # Every alert in ThirdEye is attached to an application. Please specify the registered application name here.
   # You may request for a new application by dropping an email to ask_thirdeye
   application: name_of_the_registered_application

   # The default subscription type. See note below for exploring other subscription types like dimension alerter.
   type: DEFAULT_ALERTER_PIPELINE

To see the complete list of subscription types, `click
here :ref:`all-subscription`.

b. Tell us which rules you want to subscribe to
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A subscription group in ThirdEye can subscribe to one or more detection
rules. Here you need to list the unique names of all such detection
functions. When you are creating a new alert, just copy over the
detectionName from the detection yaml which you have configured above
and paste it here.

.. code-block:: yaml

   # List of detection names that you want to subscribe.

   subscribedDetections:
   - name_of_the_detection # This is the unique name (detectionName) you defined in the detection config.
   - another_detection_name # Include more rules under the same subscription group

c. Tell us how soon you want to be alerted and the recipients
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Alert Frequency / Cron:**

Alert Frequency or cron is a way of
defining when you want to get the notification/alert for the anomaly. In
most cases users want to be notified immediately after the anomaly is
detected for which we recommend the below value. There are others who
wish to be notified at the end of the day, every hour etc. You may use
an online cronmaker if you wish to set up your own custom frequency. By
default, the cron in the config below will notify you immediately after
an anomaly is detected.

**Alert Scheme:**

Now let's define the alert scheme and the recipients
of the alerts. Alerting schemes (Email, Iris) define how a user/group
should be alerted. We recommend using the default Email based alerting
for your alerts. However, if you wish to setup Iris alerts to leverage
the power of escalation paths refer to the advanced settings section
below.

.. code-block:: yaml

   # The frequency at which you want to be notified. Typically you want to be notified immediately
   # when an anomaly is found. The below cron runs every 5 minutes.

   cron: "0 0/5 * 1/1 * ? *"

   # Configuring how you want to be alerted. You can receive the standard ThirdEye email alert (recommended)
   # or use Iris alerting to leverage the power of escalation paths. For details refer additional settings below.

   alertSchemes:

   - type: EMAIL

   # Sender of the alert. Please avoid changing this field. fromAddress: thirdeye-dev@linkedin.com
   # Configure the recipients for the email alert. We recommend putting thirdeye-dev in the cc field.

   recipients:
      to:
      - "ldap-user@linkedin.com"
      - "ldap-group@thirdeye.com"

      cc:
      - "thirdeye-dev@thirdeye.com"

      bcc:
      - "user-bcc@linkedin.com"

In theory, users can also subscribe to multiple alerting schemes. For
example, users can subscribe to either the existing thirdeye email
alerts (recommended) or Iris alerts or both.


**e. Explore more advanced subscription group settings**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`Click
here :ref:`advanced-subscription` to
explore the advanced settings.

f. A Complete notification configuration example with basic settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below is the most common example of a Third Eye notification
configuration. To see more examples,  `click
here. :ref:`templates-subscription`

.. code-block:: yaml

   # Provide a unique name to your subscription group or pick your existing subscription group from the drop-down above.
   subscriptionGroupName: thirdeye_monitoring_group

   # Every alert in ThirdEye is attached to an application. Please specify the registered application name here. Use [sandbox] only for testing.
   application: [sandbox]

   type: DEFAULT_ALERTER_PIPELINE

   cron: "0 0/5 * 1/1 * ? *"

   subscribedDetections:

   - thirdEyeTeam_thirdeyeWebapp_pinotExceptionCounter_UP # Mention the detectionName you defined in the detection configuration above

   alertSchemes:

   - type: EMAIL

   fromAddress: thirdeye-dev@linkedin.com

   recipients:
      to:
      - "user@linkedin.com" # Update the recipients
      - "group@linkedin.com"

      cc:

      - "thirdeye-dev@linkedin.com"

      referenceLinks: # Update reference links
      - "Oncall Runbook": "http://go/oncall"
      - "Thirdeye FAQs": "http://go/thirdeyefaqs"

**Step 5: Click on Create Alert to submit the alert configurations**

This is the last step of alert creation. After completing both the
configurations, click on the Create Alert button at the bottom of the
page.

Behind the scenes, we tune and replay the detection for the last 1 month
and generate historical anomalies. This will happen in the background
and you will be notified in a couple of minutes via an email about the
status of this alert. You can then view the alert along with the
historical anomalies to see how well your detection performed. You can
also choose to edit the detection and notification configurations and
tweak them further to suit to your needs.

See :ref:`advanced-detection` for more details.