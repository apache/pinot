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
 *
 */

package org.apache.pinot.thirdeye.detection.components;

import java.time.Duration;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.spec.DurationAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;


/**
 * Duration filter. Filter the anomaly based on the anomaly duration.
 * USE WITH CAUTION. If min duration is set larger than the maximum possible anomaly duration
 * the detection module produced, all anomalies would potentially be filtered.
 */
@Components(type = "DURATION_FILTER")
public class DurationAnomalyFilter implements AnomalyFilter<DurationAnomalyFilterSpec> {
  private Duration minDuration;
  private Duration maxDuration;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    long anomalyDuration = anomaly.getEndTime() - anomaly.getStartTime();
    return anomalyDuration >= this.minDuration.toMillis() && anomalyDuration <= this.maxDuration.toMillis();
  }

  @Override
  public void init(DurationAnomalyFilterSpec spec, InputDataFetcher dataFetcher) {
    if (spec.getMinDuration() != null){
      this.minDuration = Duration.parse(spec.getMinDuration());
    }
    if (spec.getMaxDuration() != null) {
      this.maxDuration = Duration.parse(spec.getMaxDuration());
    }
  }
}
