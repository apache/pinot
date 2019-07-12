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

.. _quick-start:

Quick Start
===========

ThirdEye supports an interactive demo mode for the analysis dashboard. These steps will guide you to get stepsarted.

**1: Prerequisites**


You'll need Java 8+, Maven 3.6+, and NPM 3.10+


**2: Build ThirdEye**

.. code-block:: bash

    git clone https://github.com/apache/incubator-pinot.git
    cd incubator-pinot/thirdeye
    chmod +x install.sh run-frontend.sh run-backend.sh reset.sh
    ./install.sh


Note: The build of thirdeye-frontend may take several minutes


**3: Run frontend**

.. code-block:: bash

    ./run-frontend.sh

**4: Start an analysis**

Point your favorite browser to

``http://localhost:1426/app/#/rootcause?metricId=1``

Note: ThirdEye in demo mode will accept any credentials

You will find the root cause analysis page like below:

.. image:: https://user-images.githubusercontent.com/44730481/59537777-674b0680-8eac-11e9-8c8b-314bf64d5914.png
  :width: 500

**5: Have fun**

Available metrics in demo mode are:

* business::puchases
* business::revenue
* tracking::adImpressions
* tracking::pageViews

Note: These metrics are regenerated randomly every time you launch ThirdEye in demo mode

We also have 2 real world metric with seasonality in H2 database, for detection experimentation:

* H2::daily (From: https://www.kaggle.com/marklvl/bike-sharing-dataset)
* H2::hourly (From: https://www.kaggle.com/robikscube/hourly-energy-consumption)

**6: Run detection preview**

A detection preview let you see how the detection configuration performs on past data.

Copy the following into the detection configuration:

.. code-block:: yaml


    detectionName: name_of_the_detection

    description: If this alert fires then it means so-and-so and check so-and-so for irregularities

    metric: value

    dataset: H2.H2.daily

    rules:
    - detection:
        - name: detection_rule_1
          type: HOLT_WINTERS_RULE
          params:
            sensitivity: 8

Click ``Run Preview`` button, the anomalies will be detected. Then you can play around with different time frames.

You will find the alert preview page like below:

.. image:: https://user-images.githubusercontent.com/44730481/59538138-9f9f1480-8ead-11e9-9b2f-bbb3475f27ed.png
  :width: 500

If you want to preview the hourly data, just change ``dataset: H2.H2.daily`` to ``dataset: H2.H2.hourly``, and rerun the preview.

If you want to setup in production, you need to see :ref:`configurations` and :ref:`alert-setup`.


**7: Shutdown**

You can stop the ThirdEye dashboard server anytime by pressing **Ctrl + C** in the terminal
