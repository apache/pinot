/*
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

package com.linkedin.thirdeye.detection.components;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.spec.DurationAnomalyFilterSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyFilter;
import java.time.Duration;

/**
 * Duration filter. Filter the anomaly based on anomaly duration.
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
