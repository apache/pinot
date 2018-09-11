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

package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Map;


/**
 * StaticDetectionPipeline serves as the foundation for custom detection algorithms.
 *
 * Execution takes place in three stages:
 * <ul>
 *   <li>constructor: receives configuration parameters and detection time range</li>
 *   <li>model: describes all data required to perform detection</li>
 *   <li>execution: performs any computation necessary to arrive at the detection result</li>
 * </ul>
 *
 */
public abstract class StaticDetectionPipeline extends DetectionPipeline {

  /**
   * Constructs a pipeline instance for the given configuration and detection time range
   *
   * @param provider framework data provider
   * @param config detection algorithm configuration
   * @param startTime detection start time
   * @param endTime detection end time
   */
  protected StaticDetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  /**
   * Returns a data model describing all required data to perform detection. Data is retrieved
   * in one pass and cached between executions if possible.
   *
   * @return detection data model
   */
  public abstract StaticDetectionPipelineModel getModel();

  /**
   * Returns a detection result using the data described by the data model.
   *
   * @param data data as described by data model
   * @return detection result
   * @throws Exception on execution errors
   */
  public abstract DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception;

  @Override
  public final DetectionPipelineResult run() throws Exception {
    StaticDetectionPipelineModel model = this.getModel();
    Map<MetricSlice, DataFrame> timeseries = this.provider.fetchTimeseries(model.timeseriesSlices);
    Map<MetricSlice, DataFrame> aggregates = this.provider.fetchAggregates(model.aggregateSlices, Collections.<String>emptyList());
    Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(model.anomalySlices);
    Multimap<EventSlice, EventDTO> events = this.provider.fetchEvents(model.eventSlices);

    StaticDetectionPipelineData data = new StaticDetectionPipelineData(
        model, timeseries, aggregates, anomalies, events);

    return this.run(data);
  }
}
