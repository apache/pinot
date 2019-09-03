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

Templates and examples
=========================

For step-by-step instructions and additional details on these config
files refer above sections.

.. _templates-detection:

Examples for detection configurations
-----------------------------------------

Example of a simple Threshold Rule (Min-Max)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fire an alert when the metric cross some static thresholds. For more
details about this rule see
:ref:`rule-threshold`.

.. code-block:: yaml

    detectionName: lite_liteFrontend_pageLoadTime90percentile_UP
     
    description: If this alert fires then it means that the 90 percentile page load time for p_mwlite_feed_updates has exceeded the 7 second threshold set in India. Please check the run-book to investigate.
     
    metric: page_load_time_90percentile
     
    dataset: sitespeed_thirdeye
     
    filters:
      country:
      - IN
      page_name:
      - p_mwlite_feed_updates
     
    rules:
    - detection:
      - name: detection_rule1
        type: THRESHOLD
        params:
          max: 7000


Example of a Percentage based rule 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fire an alert when the percentage change is above a certain static
threshold when comparing the current time-series with the baseline (Week
over X weeks, Median of X weeks, etc.). For more details about this rule
see :ref:`rule-percentage`.

.. code-block:: yaml

    detectionName: test_yaml_1
    description: If this alert fires then it means so-and-so and check so-and-so for irregularities
    metric: page_view
    dataset: business_intraday_metrics_dim_rt_hourly_additive
    dimensionExploration:
      dimensions:
        - browserName
     
    rules:
    - detection:                     
        - name: detection_rule_1     
          type: PERCENTAGE_RULE      
          params:                    
            offset: wo1w             
            percentageChange: 0.01

Example of multiple rules:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    detectionName: test_yaml
    description: If this alert fires then it means so-and-so and check so-and-so for irregularities
    metric: page_view
    dataset: business_intraday_metrics_dim_rt_hourly_additive
    cron: 0 0 0/1 ? * * *
    rules:
    - detection:
       - name: detection_rule_1
         type: PERCENTAGE_RULE
         params:
           offset: wo1w
           percentageChange: 0.01
     
    - detection:
       - name: detection_rule_2
         type: THRESHOLD
         params:
           min: 38000000
      filter:
       - name: filter_rule_1
         type: PERCENTAGE_CHANGE_FILTER
         params:
           offset: median4w
           threshold: 0.01
     
    - detection:
      - name: detection_rule_3
        type: HOLT_WINTERS
        params:
          sensitivity: 5

.. _templates-subscription:

Examples for subscription group configurations
--------------------------------------------------

Example of a simple subscription group 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    subscriptionGroupName: name_of_the_subscription_group
    application: name_of_the_registered_application
    type: DEFAULT_ALERTER_PIPELINE
     
    cron: "0 0/5 * 1/1 * ? *"
     
    subscribedDetections:
    - name_of_the_detection
     
    alertSchemes:
    - type: EMAIL
     
    fromAddress: thirdeye-dev@linkedin.com
    recipients:
     to:
     - "user@linkedin.com"
     - "group@linkedin.com"
     cc:
     - "thirdeye-dev@linkedin.com"
     bcc:
     - "user@linkedin.com"
     
    referenceLinks:
      "Oncall Runbook": "http://go/oncall"
      "Thirdeye FAQs": "http://go/thirdeyefaqs"


Example of a Dimensional Alerting subscription group 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    subscriptionGroupName: name_of_the_subscription_group
    application: name_of_the_registered_application
    type: DIMENSION_ALERTER_PIPELINE
     
    dimension: app_name
    dimensionRecipients:
     "android":
      - "android-oncall@linkedin.com"
     "ios":
      - "ios-oncall@linkedin.com"
     
    cron: "0 0/5 * 1/1 * ? *"
     
    subscribedDetections:
    - name_of_the_detection
     
    alertSchemes:
    - type: EMAIL
     
    fromAddress: thirdeye-dev@linkedin.com
    recipients:
     to:
     - "user@linkedin.com"
     - "group@linkedin.com"
     cc:
     - "thirdeye-dev@linkedin.com"
     bcc:
     - "user@linkedin.com"
     
    referenceLinks:
      "Oncall Runbook": "http://go/oncall"
      "Thirdeye FAQs": "http://go/thirdeyefaqs"