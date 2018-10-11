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

package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean implements AnomalyResult {

  private AnomalyFeedbackDTO feedback;

  private AnomalyFunctionDTO function;

  private Set<MergedAnomalyResultDTO> children = new HashSet<>();

  public MergedAnomalyResultDTO() {
    setCreatedTime(System.currentTimeMillis());
  }

  public MergedAnomalyResultDTO(AnomalyResult anomalyResult) {
    setCreatedTime(System.currentTimeMillis());
    populateFrom(anomalyResult);
  }

  public void populateFrom(AnomalyResult anomalyResult) {
    setStartTime(anomalyResult.getStartTime());
    setEndTime(anomalyResult.getEndTime());
    setDimensions(anomalyResult.getDimensions());
    setScore(anomalyResult.getScore());
    setWeight(anomalyResult.getWeight());
    setAvgCurrentVal(anomalyResult.getAvgCurrentVal());
    setAvgBaselineVal(anomalyResult.getAvgBaselineVal());
    setFeedback(anomalyResult.getFeedback());
    setProperties(anomalyResult.getProperties());
  }

  @Override
  public void setFeedback(AnomalyFeedback anomalyFeedback) {
    if (anomalyFeedback == null) {
      this.feedback = null;
    } else if (anomalyFeedback instanceof AnomalyFeedbackDTO) {
      this.feedback = (AnomalyFeedbackDTO) anomalyFeedback;
    } else {
      this.feedback = new AnomalyFeedbackDTO(anomalyFeedback);
    }
  }

  @Override
  public AnomalyFeedback getFeedback() {
    return this.feedback;
  }

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }

  public Set<MergedAnomalyResultDTO> getChildren() {
    return children;
  }

  public void setChildren(Set<MergedAnomalyResultDTO> children) {
    this.children = children;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MergedAnomalyResultDTO)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MergedAnomalyResultDTO that = (MergedAnomalyResultDTO) o;
    return Objects.equals(feedback, that.feedback) && Objects.equals(function, that.function) && Objects.equals(
        children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), feedback, function, children);
  }
}
