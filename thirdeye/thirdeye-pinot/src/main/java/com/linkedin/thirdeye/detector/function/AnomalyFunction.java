/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.util.AnomalyOffset;
import java.util.List;
import org.joda.time.DateTime;

public interface AnomalyFunction {
  /** Initializes this function with its configuration, call before analyze */
  void init(AnomalyFunctionDTO spec) throws Exception;

  /** Returns the specification for this function instance */
  AnomalyFunctionDTO getSpec();

  /**
   * Returns the time ranges of data that is used by this anomaly function. This method is useful when multiple time
   * intervals are needed for fetching current vs baseline data
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return the time ranges of data that is used by this anomaly function
   */
  List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime, Long monitoringWindowEndTime);

  /**
   * Analyzes a metric time series and returns any anomalous points / intervals.
   * @param exploredDimensions
   *          Pairs of dimension value and name corresponding to timeSeries.
   * @param timeSeries
   *          The metric time series data.
   * @param windowStart
   *          The beginning of the range corresponding to timeSeries.
   * @param windowEnd
   *          The end of the range corresponding to timeSeries.
   * @param knownAnomalies
   *          Any known anomalies in the time range.
   * @return
   *         A list of anomalies that were not previously known.
   */
  List<AnomalyResult> analyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception;
  /**
   * Analyzes a metric time series before windowStart and returns any anomalous points / intervals.
   * @param exploredDimensions
   *          Pairs of dimension value and name corresponding to timeSeries.
   * @param timeSeries
   *          The metric time series data.
   * @param windowStart
   *          The beginning of the range corresponding to timeSeries.
   * @param windowEnd
   *          The end of the range corresponding to timeSeries.
   * @param knownAnomalies
   *          Any known anomalies in the time range.
   * @return
   *         A list of anomalies that were not previously known.
   */

  List<AnomalyResult> offlineAnalyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception;

  /**
   * Computes the score and severity according to the current and baseline of the given timeSeries and stores the
   * information to the merged anomaly. The start and end time of the time series is provided
   *
   * @param anomalyToUpdated
   *          the merged anomaly to be updated.
   * @param timeSeries
   *          The metric time series data.
   * @param knownAnomalies
   *          Any known anomalies in the time range.
   * @return
   *         A list of anomalies that were not previously known.
   * @return the severity according to the current and baseline of the given timeSeries
   */
  void updateMergedAnomalyInfo(MergedAnomalyResultDTO anomalyToUpdated, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception;

  /**
   * Given any metric, this method returns the corresponding current and baseline time series to
   * be presented in the frontend. The given metric is not necessary the data that is used for
   * detecting anomaly.
   *
   * For instance, if a function uses the average values of the past 3 weeks as the baseline for
   * anomaly detection, then this method should construct a baseline that contains the average
   * value of the past 3 weeks of any given metric.
   *
   * Note that the usage of this method should be similar to the method {@link #analyze}, i.e., it
   * does not take care of setting filters, dimension names, etc. for retrieving the data from the
   * backend database. Specifically, it only processes the given data, i.e., timeSeries, for
   * presentation purpose.
   *
   * The only difference between this method and {@link #analyze} is that their bucket sizes are
   * different. This method's bucket size is given by frontend, which should larger or equal to the
   * minimum time granularity of the data. On the other hand, {@link #analyze}'s buckets size is
   * always the minimum time granularity of the data.
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param metric the metric name to retrieve the data from the given time series
   * @param bucketMillis the size of a bucket in milli-seconds
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @return Two sets of time series: a current and a baseline values, to be represented in the frontend
   */
  AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis,
      String metric, long viewWindowStartTime, long viewWindowEndTime,
      List<MergedAnomalyResultDTO> knownAnomalies);

  /**
   *
   * @return List of property keys applied in case of specific anomaly function
   */
  String[] getPropertyKeys();

  /**
   * This method is added to support the viewing of anomalies on the dashboard.
   * When viewing anomalies, we need to fetch more data than just the anomaly
   * region (typically some buffer before and after the anomaly region).
   * This fetched data will be transformed, and then used to display on the front end.
   * The data we need to fetch may not follow a simple rule
   * such as, "fetch x number of days before and after for this granularity".
   * We have come to see that this data to fetch, might depend on which function is fetching it,
   * as some functions are designed to already fetch all data they need, and no offset needs to be supplied.
   * Hence it is best that the function makes the decision of how much data to fetch.
   *
   * To summarize, this method will allow us to decide the offset for an anomaly region,
   * and append appropriate padding before and after the anomaly region, while FETCHING DATA FOR IT
   *
   * @param datasetConfig
   * @return
   */
  AnomalyOffset getAnomalyWindowOffset(DatasetConfigDTO datasetConfig);

  /**
   * This method is added to support the viewing of anomalies on the dashboard.
   * When displaying anomalies, we first fetch the data for the baseline and current.
   * Some functions fetch more historical data than just the anomaly window provided.
   * But when displaying, we should display only some of it, typically some padding before and after the anomaly region
   * This is again logic which is specific to the function requesting it.
   * Hence it is best that the function makes the decision of how much of the fetched data it wants to actually display.
   *
   * To summarize, this method will allow us to decide the offset for an anomaly region,
   * and append appropriate padding before and after the anomaly region, while VIEWING IT
   * @param datasetConfig
   * @return
   */
  AnomalyOffset getViewWindowOffset(DatasetConfigDTO datasetConfig);
}
