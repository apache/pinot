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

package com.linkedin.thirdeye.anomalydetection.context;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import java.util.Collections;
import java.util.Map;


/**
 * An simple class for storing the detection result from anomaly functions. An instance of RawAnomalyResult is
 * supposed to be used to update or create an existing or new merged anomaly, respectively.
 */
public class RawAnomalyResult implements AnomalyResult {
  private long startTime;
  private long endTime;
  private DimensionMap dimensions = new DimensionMap();

  private double score; // confidence level
  private double weight; // severity
  private double avgCurrentVal;
  private double avgBaselineVal;
  private Map<String, String> properties = Collections.emptyMap();
  private AnomalyFeedbackDTO feedback = null;


  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public long getEndTime() {
    return endTime;
  }

  @Override
  public void setDimensions(DimensionMap dimensionMap) {
    this.dimensions = Preconditions.checkNotNull(dimensionMap);
  }

  @Override
  public DimensionMap getDimensions() {
    return dimensions;
  }

  @Override
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public double getScore() {
    return score;
  }

  @Override
  public void setScore(double score) {
    this.score = score;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public void setWeight(double weight) {
    this.weight = weight;
  }

  @Override
  public double getAvgCurrentVal() {
    return avgCurrentVal;
  }

  @Override
  public void setAvgCurrentVal(double avgCurrentVal) {
    this.avgCurrentVal = avgCurrentVal;
  }

  @Override
  public double getAvgBaselineVal() {
    return avgBaselineVal;
  }

  @Override
  public void setAvgBaselineVal(double avgBaselineVal) {
    this.avgBaselineVal = avgBaselineVal;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = Preconditions.checkNotNull(properties);
  }

  @Override
  public AnomalyFeedback getFeedback() {
    return feedback;
  }

  @Override
  public void setFeedback(AnomalyFeedback feedback) {
    if (feedback == null) {
      this.feedback = null;
    } else if (feedback instanceof AnomalyFeedbackDTO) {
      this.feedback = (AnomalyFeedbackDTO) feedback;
    } else {
      this.feedback = new AnomalyFeedbackDTO(feedback);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("startTime", startTime)
        .add("endTime", endTime)
        .add("dimensions", dimensions)
        .add("score", score)
        .add("weight", weight)
        .add("avgCurrentVal", avgCurrentVal)
        .add("avgBaselineVal", avgBaselineVal)
        .add("properties", properties)
        .add("feedback", feedback)
        .toString();
  }
}
