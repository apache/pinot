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

Appendix
========
.. _all-detection-rules:

A. List of all supported detection rules:
-----------------------------------------

.. _rule-percentage:

1.  Percentage Rule (type: PERCENTAGE\_RULE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compares current time series to a baseline, if the percentage change is
above a certain threshold, detect it as an anomaly.

Example:

.. code-block:: yaml

    rules:
    - detection:
        - name: detection_rule_1
          type: PERCENTAGE_RULE
          params:
            offset: wo1w
            percentageChange: 0.1
            pattern: UP_OR_DOWN

**Parameters:**

+--------------------+--------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| **param**          | **description**                                                                                        | **default value**   | **supported values**                                                                                              |
+====================+========================================================================================================+=====================+===================================================================================================================+
| offset             | the baseline time series to compare with.                                                              | wo1w                | \* **hoXh** hour-over-hour data points with a lag of X hours                                                      |
|                    |                                                                                                        |                     | \* **doXd** day-over-day data points with a lag of X days                                                         |
|                    |                                                                                                        |                     | \* **woXw** week-over-week data points with a lag of X weeks                                                      |
|                    |                                                                                                        |                     | \* **moXm** month-over-month data points with a lag of X months                                                   |
|                    |                                                                                                        |                     | \* **meanXU** average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)    |
|                    |                                                                                                        |                     | \* **medianXU** median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)   |
|                    |                                                                                                        |                     | \* **minXU** minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)     |
|                    |                                                                                                        |                     | \* **maxXU** maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)     |
+--------------------+--------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| percentageChange   | The percentage threshold. If the percentage change is above this threshold, detect it as an anomaly.   | NaN                 | double values                                                                                                     |
|                    |                                                                                                        |                     |                                                                                                                   |
|                    |                                                                                                        |                     | NaN means no threshold set.                                                                                       |
+--------------------+--------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| pattern            | Detect as an anomaly if the metric drop, rise or both directions.                                      | UP\_OR\_DOWN        | UP: detect as an anomaly only if the current time series is above the baseline.                                   |
|                    |                                                                                                        |                     |                                                                                                                   |
|                    |                                                                                                        |                     | DOWN: detect as an anomaly only if the current time series is below the baseline.                                 |
|                    |                                                                                                        |                     |                                                                                                                   |
|                    |                                                                                                        |                     | UP\_OR\_DOWN: detect as an anomaly in both directions                                                             |
+--------------------+--------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+

.. _rule-threshold:

2. Threshold Rule (type: THRESHOLD)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If metric is above the max threshold or below the min threshold, detect
is as an anomaly.

Example:

.. code-block:: yaml

    rules:
    - detection:
        - name: detection_rule_1
          type: THRESHOLD
          params:
            max: 1000
            min: NaN

**Parameters:**

+--------------+------------------------------------------------------------------+---------------------+-------------------------------+
| **params**   | **description**                                                  | **default value**   | **supported values**          |
+==============+==================================================================+=====================+===============================+
| max          | If the metric goes above this value, detect is as an anomaly.    | NaN                 | double values                 |
|              |                                                                  |                     |                               |
|              |                                                                  |                     | NaN means no threshold set.   |
+--------------+------------------------------------------------------------------+---------------------+-------------------------------+
| min          | If the metric goes above below value, detect is as an anomaly.   | NaN                 | double values                 |
|              |                                                                  |                     |                               |
|              |                                                                  |                     | NaN means no threshold set.   |
+--------------+------------------------------------------------------------------+---------------------+-------------------------------+

.. _rule-holtwinters:

3.  Holt-Winters Algorithm (type: HOLT\_WINTERS\_RULE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Holt-Winters Algorithm <https://otexts.com/fpp2/holt-winters.html>`__ is a commonly used statistic forecasting algorithm for
anomaly detection.

This algorithm performs very well for daily data and monthly data.

For hourly data and minutely data, please trial and error more patiently
with duration filters and percentage filters.

**Minimal configuration (for any granularity):**

.. code-block:: yaml

    rules:
    - detection:
        - name: detection_rule_1
          type: HOLT_WINTERS_RULE
          params:
            sensitivity: 6 # Detection sensitivity scale from 0 - 10, mapping z-score from 1 to 3.
            pattern: UP_OR_DOWN # Alert when value goes up or down by the configured threshold. (Values supported - UP, DOWN, UP_OR_DOWN)

**Optional Parameters:**

+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| **param**         | **description**                                                                                      | **default value**                                                                            | **supported values**   |
+===================+======================================================================================================+==============================================================================================+========================+
| sensitivity       | Detection sensitivity scale from 0 - 10, mapping z-score from 1 to 3.                                | 5                                                                                            | any double in [0, 10]  |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| pattern           | Detect as an anomaly if the metric drop, rise or both directions.                                    | UP_OR_DOWN                                                                                   | UP, DOWN, UP_OR_DOWN   |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| alpha             | level smoothing factor                                                                               | Optimized by `BOBYQA optimizer <https://en.wikipedia.org/wiki/BOBYQA>`__ to minimize error   | any double in [0, 1]   |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| beta              | trend smoothing factor                                                                               | Optimized by `BOBYQA optimizer <https://en.wikipedia.org/wiki/BOBYQA>`__ to minimize error   | any double in [0, 1]   |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| gamma             | seasonal smoothing factor                                                                            | Optimized by `BOBYQA optimizer <https://en.wikipedia.org/wiki/BOBYQA>`__ to minimize error   | any double in [0, 1]   |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| period            | seasonality period, default 7 for daily, hourly and minutely data. For monthly data, set it to 12.   | 7                                                                                            | Any positive interger  |
|                   | For non-seasonal data, set it to 1.                                                                  |                                                                                              |                        |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+
| smoothing        | For smoothing of hourly and minutely data to reduce noise                                            | true                                                                                         | true or false          |
+-------------------+------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------+

.. _rule-absolutechange:

4. Absolute change Rule (Type: ABSOLUTE\_CHANGE\_RULE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compares current time series to a baseline, if the absolute change is
above a certain threshold, detect it as an anomaly.

Example:

.. code-block:: yaml

    rules:
    - detection:
        - name: detection_rule_1
          type: ABSOLUTE_CHANGE_RULE
          params:
            offset: wo1w
            absoluteChange: 100
            pattern: UP_OR_DOWN

**Parameters:**

+------------------+-----------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| **param**        | **description**                                                                                                                         | **default value**   | **supported values**                                                                                              |
+==================+=========================================================================================================================================+=====================+===================================================================================================================+
| offset           | the baseline time series to compare with.                                                                                               | wo1w                | \* **hoXh** hour-over-hour data points with a lag of X hours                                                      |
|                  |                                                                                                                                         |                     | \* **doXd** day-over-day data points with a lag of X days                                                         |
|                  |                                                                                                                                         |                     | \* **woXw** week-over-week data points with a lag of X weeks                                                      |
|                  |                                                                                                                                         |                     | \* **moXm** month-over-month data points with a lag of X months                                                   |
|                  |                                                                                                                                         |                     | \* **meanXU** average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)    |
|                  |                                                                                                                                         |                     | \* **medianXU** median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)   |
|                  |                                                                                                                                         |                     | \* **minXU** minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)     |
|                  |                                                                                                                                         |                     | \* **maxXU** maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)     |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| absoluteChange   | The absolute change threshold. If the absolute change when compared to the baseline is above this threshold, detect it as an anomaly.   | NaN                 | double values                                                                                                     |
|                  |                                                                                                                                         |                     |                                                                                                                   |
|                  |                                                                                                                                         |                     | NaN means no threshold set.                                                                                       |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+
| pattern          | Detect as an anomaly if the metric drop, rise or both directions.                                                                       | UP\_OR\_DOWN        | UP: detect as an anomaly only if the current time series is above the baseline.                                   |
|                  |                                                                                                                                         |                     |                                                                                                                   |
|                  |                                                                                                                                         |                     | DOWN: detect as an anomaly only if the current time series is below the baseline.                                 |
|                  |                                                                                                                                         |                     |                                                                                                                   |
|                  |                                                                                                                                         |                     | UP\_OR\_DOWN: detect as an anomaly in both directions                                                             |
+------------------+-----------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------------------------------------------------------------------------------------------+

.. _all-filter-rules:

B. List of all supported filter rules
-----------------------------------------

.._filter-percentage:

1. Percentage change anomaly filter (type: PERCENTAGE\_CHANGE\_FILTER)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filter the anomaly if compared to the baseline, percentage change is
below a certain threshold. 

Example:

.. code-block:: yaml

    filter:
      - name: filter_rule_1
        type: PERCENTAGE_CHANGE_FILTER
        params:
          threshold: 0.1 # filter out all changes less than 10%

**Parameters:**

+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| **params**   | **description**                                                                                   | **default value**                                   | **supported values**                                                                                                                                                                                                                                     |
+==============+===================================================================================================+=====================================================+==========================================================================================================================================================================================================================================================+
| threshold    | The percentage threshold. If the percentage change is below this threshold, filter the anomaly.   | NaN                                                 | double values                                                                                                                                                                                                                                            |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | NaN means no threshold set.                                                                                                                                                                                                                              |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| offset       | The baseline timeseries used to calculate the baseline value.                                     | The default baseline used in detection algorithm.   | | \* \ **hoXh** hour-over-hour data points with a lag of X hours                                                                                                                                                                                         |
|              |                                                                                                   |                                                     | | \* \ **doXd** day-over-day data points with a lag of X days                                                                                                                                                                                            |
|              |                                                                                                   |                                                     | | \* \ **woXw** week-over-week data points with a lag of X weeks                                                                                                                                                                                         |
|              |                                                                                                   |                                                     | | \* \ **moXm** month-over-month data points with a lag of X months                                                                                                                                                                                      |
|              |                                                                                                   |                                                     | | \* \ **meanXU** average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                       |
|              |                                                                                                   |                                                     | | \* \ **medianXU** median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                      |
|              |                                                                                                   |                                                     | | \* \ **minXU** minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                        |
|              |                                                                                                   |                                                     | | \* \ **maxXU** maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                        |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | **If this value is not set, it will use the default baseline**. E.g, if the detection uses PERCENTAGE\_RULE and offset is wo1w then the baseline is last week's value. If the detection type is ALGORITHM then the baseline is generated by algorithm.   |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| pattern      | Keep as an anomaly if the metric drop, rise or both directions.                                   | UP\_OR\_DOWN                                        | UP: Keep the anomaly only if the current value is above the baseline and passes the threshold.                                                                                                                                                           |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | DOWN: Keep the anomaly only if the current value is below the baseline and passes the threshold.                                                                                                                                                         |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | UP\_OR\_DOWN: Keep the anomaly if it passes the threshold regardless of metric moving to which directions                                                                                                                                                |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

.._filter-sitewide:

2. Site wide impact anomaly filter (Type: SITEWIDE\_IMPACT\_FILTER)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filter the anomaly if its site wide impact is below a certain
threshold. 

How site wide impact is calculated?

SWI = (currentValue of the anomaly - baselineValue of the anomaly) /
(current value of the site wide metric in the anomaly range)

Example: 

In the following example, we are setting up an anomaly detection
pipeline for all the possible platforms (such as ios, android, windows,
etc) in the US. We use the percentage rule to detect the anomaly, if the
metric compared to median over 4 weeks value is up or down 1%, and the
site-wide impact for the anomaly is larger than 1%, we say this is an
anomaly.

For example, an anomaly is detected in iOS platform , the anomaly
happens 2pm to 3pm. The site wide impact is calculated by: Taking the
the total number of sign ups on iOS in U.S. between 2 to 3 pm, minus the
week over week baseline value between 2 to 3 pm and then divided
the current signup value of U.S. among all platforms.

.. code-block:: yaml

  detectionName: swi_monitor
  metric: signups
  dataset: registration_metrics_v2_additive
  dimensionExploration:
   dimensions:
      platform
  filters:
      country:
        us
  rules:
  - detection:
      - name: detection_rule1
        type: PERCENTAGE_RULE
        params:
          offset: median4w
          percentageChange: 0.01
    filter:
     - type: SITEWIDE_IMPACT_FILTER
       name: filter_rule_1
       params:
          threshold: 0.01
          pattern: up_or_down
          offset: wo1w
          sitewideMetricName: signups
          sitewideCollection: registration_metrics_v2_additive
          filters:
              country:
                  us


**Parameters:**

+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| **params**           | **description**                                                                                   | **default value**                                                               | **supported values & descriptions**                                                                                     |
+======================+===================================================================================================+=================================================================================+=========================================================================================================================+
| threshold            | The percentage threshold. If the percentage change is below this threshold, filter the anomaly.   | NaN                                                                             | double values                                                                                                           |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | NaN means no threshold set.                                                                                             |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| pattern              | Keep as an anomaly if the metric drop, rise or both directions.                                   | UP\_OR\_DOWN                                                                    | UP: Keep the anomaly only if the current value is above the baseline and passes the threshold.                          |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | DOWN: Keep the anomaly only if the current value is below the baseline and passes the threshold.                        |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | UP\_OR\_DOWN: Keep the anomaly if it passes the threshold regardless of metric moving to which directions               |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| sitewideMetricName   | The metric to calculate the site wide baseline value                                              | By default, use the same metric as the anomaly without the dimension filters.   | All metric names in ThirdEye.                                                                                           |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| sitewideCollection   | The metric to calculate the site wide baseline value                                              | By default, use the same metric as the anomaly without the dimension filters.   | The dataset name for the site wide metric. The                                                                          |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | sitewideCollection must be configured together with the                                                                 |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | sitewideMetricName.                                                                                                     |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| filters              | The dimension filter for the site wide metric                                                     | By default, use the same metric as the anomaly without the dimension filters.   | See Dimension filter to configure the filters for site wide metric. This filters must be configured together with the   |
|                      |                                                                                                   |                                                                                 |                                                                                                                         |
|                      |                                                                                                   |                                                                                 | sitewideMetricName.                                                                                                     |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+
| offset               | The baseline time series used to calculate the baseline value.                                    | Use the baseline value generated in detection for the anomaly.                  | \* \ **hoXh** hour-over-hour data points with a lag of X hours                                                          |
|                      |                                                                                                   |                                                                                 | \* \ **doXd** day-over-day data points with a lag of X days                                                             |
|                      |                                                                                                   |                                                                                 | \* \ **woXw** week-over-week data points with a lag of X weeks                                                          |
|                      |                                                                                                   |                                                                                 | \* \ **moXm** month-over-month data points with a lag of X months                                                       |
|                      |                                                                                                   |                                                                                 | \* \ **meanXU** average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)        |
|                      |                                                                                                   |                                                                                 | \* \ **medianXU** median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)       |
|                      |                                                                                                   |                                                                                 | \* \ **minXU** minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)         |
|                      |                                                                                                   |                                                                                 | \* \ **maxXU** maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)         |
+----------------------+---------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------+

.. _filter-threshold:

3. Threshold-based anomaly filter (Type: THRESHOLD\_RULE\_FILTER)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filter the anomaly if the metric current value in the anomaly time
duration is outside of the allowed range.

For example:

Filter the anomaly, if the anomaly current value per hour is less than
1000 or larger than 2000, filter the anomaly.

.. code-block:: yaml

  filter:
      - name: filter_rule_1
        type: THRESHOLD_RULE_FILTER
        params:
          minValueHourly: 1000
          maxValueHourly: 2000

**Parameters:**

+------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------+
| **params**       | **description**                                                                                                                                                     | **default value**   | **supported values**          |
+==================+=====================================================================================================================================================================+=====================+===============================+
| minValueHourly   | The minimum value allowed for an anomaly on an hourly bases. If the current value per hour in the anomaly duration is less than this value, filter the anomaly.     | NaN                 | double values                 |
|                  |                                                                                                                                                                     |                     |                               |
|                  |                                                                                                                                                                     |                     | NaN means no threshold set.   |
+------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------+
| maxValueHourly   | The maximum value allowed for an anomaly on an hourly bases. If the current value per hour in the anomaly duration is larger than this value, filter the anomaly.   | NaN                 | double values                 |
|                  |                                                                                                                                                                     |                     |                               |
|                  |                                                                                                                                                                     |                     | NaN means no threshold set.   |
+------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------+
| minValueDaily    | The minimum value allowed for an anomaly on a daily bases. If the current value per day in the anomaly duration is less than this value, filter the anomaly.        | NaN                 | double values                 |
|                  |                                                                                                                                                                     |                     |                               |
|                  |                                                                                                                                                                     |                     | NaN means no threshold set.   |
+------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------+
| maxValueDaily    | The maximum value allowed for an anomaly on a daily bases. If the current value per day in the anomaly duration is larger than this value, filter the anomaly.      | NaN                 | double values                 |
|                  |                                                                                                                                                                     |                     |                               |
|                  |                                                                                                                                                                     |                     | NaN means no threshold set.   |
+------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------+-------------------------------+

.. _filter-duration:

4. Anomaly duration filter (Type: DURATION\_FILTER)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filter the anomalies based on the anomaly duration. 

**Parameters:**

+---------------+-------------------------------------------------------------------------------------------------------------------------+---------------------+------------------------------------------------------------------------------------------------------------------+
| **params**    | **description**                                                                                                         | **default value**   | **supported values**                                                                                             |
+===============+=========================================================================================================================+=====================+==================================================================================================================+
| minDuration   | The minimum duration allowed for an anomaly. If the anomaly's duration is less than this value, filter the anomaly.     | null                | String representation of Java duration.                                                                          |
|               |                                                                                                                         |                     |                                                                                                                  |
|               |                                                                                                                         |                     | See examples here:                                                                                               |
|               |                                                                                                                         |                     |                                                                                                                  |
|               |                                                                                                                         |                     | http://www.java2s.com/Tutorials/Java_Date_Time/java.time/Duration/Duration_parse_CharSequence_text_example.htm   |
+---------------+-------------------------------------------------------------------------------------------------------------------------+---------------------+------------------------------------------------------------------------------------------------------------------+
| maxDuration   | The maximum duration allowed for an anomaly. If the anomaly's duration is larger than this value, filter the anomaly.   | null                | String representation of Java duration                                                                           |
+---------------+-------------------------------------------------------------------------------------------------------------------------+---------------------+------------------------------------------------------------------------------------------------------------------+

For example:

Filter the anomaly, if the anomaly duration is less than 15 minutes.

.. code-block:: yaml

  filter:
      - name: filter_rule_1
        type: DURATION_FILTER
        params:
          minDuration: PT15M

Please override the default merge configs in the YAML if the duration
filter is set. Otherwise, it might have side effects. 

.. code-block:: yaml

  merger:
    maxGap: 0  # prevent potential anomaly duration extension

.._filter-absolutechange
5. Absolute change anomaly filter (Type: ABSOLUTE\_CHANGE\_FILTER)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 Check if the anomaly's absolute change compared to baseline is above
the threshold If not, filters the anomaly.

Example:

.. code-block:: yaml

  filter:
      - name: filter_rule_1
        type: ABSOLUTE_CHANGE_FILTER
        params:
          threshold: 0.1 # filter out all changes less than 10%

**Parameters:**

+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| **params**   | **description**                                                                                   | **default value**                                   | **supported values**                                                                                                                                                                                                                                     |
+==============+===================================================================================================+=====================================================+==========================================================================================================================================================================================================================================================+
| threshold    | The percentage threshold. If the percentage change is below this threshold, filter the anomaly.   | NaN                                                 | double values                                                                                                                                                                                                                                            |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | NaN means no threshold set.                                                                                                                                                                                                                              |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| offset       | The baseline timeseries used to calculate the baseline value.                                     | The default baseline used in detection algorithm.   | | \* \ **hoXh** hour-over-hour data points with a lag of X hours                                                                                                                                                                                         |
|              |                                                                                                   |                                                     | | \* \ **doXd** day-over-day data points with a lag of X days                                                                                                                                                                                            |
|              |                                                                                                   |                                                     | | \* \ **woXw** week-over-week data points with a lag of X weeks                                                                                                                                                                                         |
|              |                                                                                                   |                                                     | | \* \ **moXm** month-over-month data points with a lag of X months                                                                                                                                                                                      |
|              |                                                                                                   |                                                     | | \* \ **meanXU** average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                       |
|              |                                                                                                   |                                                     | | \* \ **medianXU** median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                      |
|              |                                                                                                   |                                                     | | \* \ **minXU** minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                        |
|              |                                                                                                   |                                                     | | \* \ **maxXU** maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)                                                                                                                                        |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | **If this value is not set, it will use the default baseline**. E.g, if the detection uses PERCENTAGE\_RULE and offset is wo1w then the baseline is last week's value. If the detection type is ALGORITHM then the baseline is generated by algorithm.   |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| pattern      | Keep as an anomaly if the metric drop, rise or both directions.                                   | UP\_OR\_DOWN                                        | UP: Keep the anomaly only if the current value is above the baseline and passes the threshold.                                                                                                                                                           |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | DOWN: Keep the anomaly only if the current value is below the baseline and passes the threshold.                                                                                                                                                         |
|              |                                                                                                   |                                                     |                                                                                                                                                                                                                                                          |
|              |                                                                                                   |                                                     | UP\_OR\_DOWN: Keep the anomaly if it passes the threshold regardless of metric moving to which directions                                                                                                                                                |
+--------------+---------------------------------------------------------------------------------------------------+-----------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. _all-subscription:

C. List of all supported Subscription group Types
-------------------------------------------------

1. Default Alerter (type: DEFAULT_ALERTER_PIPELINE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default notification type which lets you to configure a set of recipients and sends anomaly notification to all of them.

.. code-block:: yaml

  type: DEFAULT_ALERTER_PIPELINE

2. Dimension Alerter (type: DIMENSION_ALERTER_PIPELINE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This gives you the ability to alert different people/group/team based on the dimension values. This is a special notification type which sends the anomaly email to a set of unconditional and another set of conditional recipients, based on the value of a specified anomaly dimension.

.. code-block:: yaml

  type: DIMENSION_ALERTER_PIPELINE
  dimension: app_name
  dimensionRecipients:
   "android":
    - "android-oncall@linkedin.com"
   "ios":
    - "ios-oncall@linkedin.com"

