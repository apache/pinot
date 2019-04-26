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

.. _contribute-detection:

Didn't Find the Detection Algorithm You Want? Contribute!
==========================================================

With the interfaces ThirdEye provide, it is not too difficult to add a new detection rule or algorithm.
When you are implementing, you can refer to ``thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/detection/components/HoltWintersDetector.java``.
Contributions are highly welcomed. If you have any question, feel free to contact us.

1. Add Configuration Spec
~~~~~~~~~~~~~~~~~~~~~~~~~

Refer to ``thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/detection/spec/HoltWintersDetectorSpec``
, add all the configurable parameters of the rule/algorithm.

2. Implement Detection Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Refer to ``thirdeye-pinot/src/main/java/org/apache/pinot/thirdeye/detection/components/HoltWintersDetector.java``,
implements ``BaseLineProvider`` and ``AnomalyDetector`` interface accordingly.

``BaseLineProvider`` has function ``computePredictedTimeSeries``, which returns a TimeSeries that contains the predicted baseline and upper and lower bounds of the predicted baseline.

``AnomalyDetector`` has function ``runDetection``, which returns a list of anomalies detected by the algorithm/rule.

3. Preview Your Algorithm
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In ``Create Alert`` detection configuration yaml, change the detection type to the type you specified in your detection class decorator. 
Enter your params and try it out on any metric!