/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection;

import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;


/**
 * Centralized data source for anomaly detection algorithms. All data used by any
 * algorithm <b>MUST</b> be obtained through this interface to maintain loose coupling.
 *
 * <br/><b>NOTE:</b> extend this interface in case necessary data cannot be obtained
 * through one of the existing methods.
 */
public interface DataProvider {
  /**
   * Returns a map of granular timeseries (keyed by slice) for a given set of slices.
   * The format of the DataFrame follows the standard convention of DataFrameUtils.
   *
   * @see MetricSlice
   * @see org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils
   *
   * @param slices metric slices
   * @return map of timeseries (keyed by slice)
   */
  Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices);

  /**
   * Returns a map of aggregation values (keyed by slice) for a given set of slices,
   * grouped by the given dimensions.
   * The format of the DataFrame follows the standard convention of DataFrameUtils.
   *
   * @see MetricSlice
   * @see org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils
   *
   * @param slices metric slices
   * @param dimensions dimensions to group by
   * @return map of aggregation values (keyed by slice)
   */
  Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, List<String> dimensions);

  /**
   * Returns a multimap of anomalies (keyed by slice) for a given set of slices.
   *
   * @see MergedAnomalyResultDTO
   * @see AnomalySlice
   *
   * @param slices anomaly slice
   * @return multimap of anomalies (keyed by slice)
   */
  Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices);

  /**
   * Returns a multimap of events (keyed by slice) for a given set of slices.
   *
   * @see EventDTO
   * @see EventSlice
   *
   * @param slices event slice
   * @return multimap of events (keyed by slice)
   */
  Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices);

  /**
   * Returns a map of metric configs (keyed by id) for a given set of ids.
   *
   * @see MetricConfigDTO
   *
   * @param ids metric config ids
   * @return map of metric configs (keyed by id)
   */
  Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids);


  /**
   * Returns a map of dataset configs (keyed by id) for a given set of dataset names.
   *
   * @see DatasetConfigDTO
   *
   * @param datasetNames dataset config names
   * @return map of dataset configs (keyed by dataset name)
   */
  Map<String, DatasetConfigDTO> fetchDatasets(Collection<String> datasetNames);

  /**
   * Returns a metricConfigDTO for a given metric name.
   *
   * @see MetricConfigDTO
   *
   * @param metricName metric name
   * @param datasetName dataset name
   * @return map of dataset configs (keyed by dataset name)
   */
  MetricConfigDTO fetchMetric(String metricName, String datasetName);

  /**
   * Returns an initialized instance of a detection pipeline for the given config. Injects this
   * DataProvider as provider for the new pipeline.
   *
   * <br/><b>NOTE:</b> this method is typically used for prototyping and creating nested pipelines
   *
   * @param config detection config
   * @param start detection window start time
   * @param end detection window end time
   * @return detection pipeline instance
   * @throws Exception
   */
  DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception;

  /**
   * Returns a multimap of evaluations (keyed by the evaluations slice) for a given set of evaluations slices.
   *
   * @see Evaluation
   *
   * @param evaluationSlices the evaluation slices
   * @param configId configId
   * @return a multimap of evaluations (keyed by the evaluations slice)
   */
  Multimap<EvaluationSlice, EvaluationDTO> fetchEvaluations(Collection<EvaluationSlice> evaluationSlices, long configId);

  default List<DatasetConfigDTO> fetchDatasetByDisplayName(String datasetDisplayName) {
    throw new NotImplementedException("the method is not implemented");
  }
}
