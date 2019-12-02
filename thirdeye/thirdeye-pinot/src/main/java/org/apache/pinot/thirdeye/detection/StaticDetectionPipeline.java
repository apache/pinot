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
import java.util.HashSet;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import java.util.Collections;
import java.util.Map;


/**
 * StaticDetectionPipeline serves as the foundation for custom detection algorithms.
 *
 * Execution takes place in three stages:
 * <ul>
 *   <li>constructor: receives configuration parameters and detection time range</li>
 *   <li>dataSpec: describes all data required to perform detection</li>
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
   * Returns a data spec describing all required data to perform detection. Data is retrieved
   * in one pass and cached between executions if possible.
   *
   * @return detection data spec
   */
  public abstract InputDataSpec getInputDataSpec();

  /**
   * Returns a detection result using the data described by the data dataSpec.
   *
   * @param data data as described by data dataSpec
   * @return detection result
   * @throws Exception on execution errors
   */
  public abstract DetectionPipelineResult run(InputData data) throws Exception;

  @Override
  public final DetectionPipelineResult run() throws Exception {
    InputDataSpec dataSpec = this.getInputDataSpec();
    Map<MetricSlice, DataFrame> timeseries = this.provider.fetchTimeseries(dataSpec.getTimeseriesSlices());
    Map<MetricSlice, DataFrame> aggregates = this.provider.fetchAggregates(dataSpec.getAggregateSlices(), Collections.<String>emptyList());

    Collection<AnomalySlice> updatedSlices = new HashSet<>();
    for (AnomalySlice slice : dataSpec.getAnomalySlices()) {
      updatedSlices.add(slice.withDetectionId(this.config.getId()));
    }
    Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(updatedSlices);
    Multimap<EventSlice, EventDTO> events = this.provider.fetchEvents(dataSpec.getEventSlices());

    InputData data = new InputData(
        dataSpec, timeseries, aggregates, anomalies, events);

    return this.run(data);
  }
}
