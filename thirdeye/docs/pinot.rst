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

.. _pinot:

Pinot
=====

**0: Prerequisites**

Run through the **Quick Start** guide and shut down the frontend server process.


**1: Update the data sources configuration**

Insert the connector configuration for Pinot in `thirdeye-pinot/config/data-sources/data-sources-config.yml`. Your config should look like this:

.. code-block:: yaml

    dataSourceConfigs:
      - className: com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource
        properties:
            zookeeperUrl: 'myZkCluster.myDomain:12913/pinot-cluster'
            clusterName: 'myDemoCluster'
            controllerConnectionScheme: 'https'
            controllerHost: 'myPinotController.myDomain'
            controllerPort: 10611
            cacheLoaderClassName: com.linkedin.thirdeye.datasource.pinot.PinotControllerResponseCacheLoader
        metadataSourceConfigs:
          - className: com.linkedin.thirdeye.auto.onboard.AutoOnboardPinotMetadataSource

      - className: com.linkedin.thirdeye.datasource.mock.MockThirdEyeDataSource
        ...


Note: You'll have to change the host names and port numbers according to your setup


**2: Enable Pinot auto-onboarding**

Update the `thirdeye-pinot/config/detector.yml` file to enable auto onboarding of pinot data sets.

.. code-block:: yaml

    autoload: true



**3: Run the backend worker to load all supported Pinot data sets**

.. code-block:: bash

    ./run-backend.sh


Note: This process may take some time. The worker process will print log messages for each data set schema being processed. Schemas must contain a `timeFieldSpec` or a `dateTimeFieldSpec` in order for ThirdEye to onboard it automatically


**4: Stop the backend worker**

By pressing **Ctrl-C** in the terminal


**5: Run ThirdEye frontend**

.. code-block:: bash

    ./run-frontend.sh
