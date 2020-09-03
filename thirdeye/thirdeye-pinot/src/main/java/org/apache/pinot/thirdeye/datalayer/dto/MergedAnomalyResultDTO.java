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

package org.apache.pinot.thirdeye.datalayer.dto;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.Serializable;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResultDTO extends MergedAnomalyResultBean implements AnomalyResult, Serializable {
  public static final String ISSUE_TYPE_KEY = "issue_type";
  public static final String TIME_SERIES_SNAPSHOT_KEY = "anomalyTimelinesView";

  private AnomalyFeedbackDTO feedback;

  private AnomalyFunctionDTO function;

  private Set<MergedAnomalyResultDTO> children = new HashSet<>();

  // flag to be set when severity changes but not to be persisted
  private boolean renotify = false;

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
    setScore(anomalyResult.getScore());
    setWeight(anomalyResult.getWeight());
    setAvgCurrentVal(anomalyResult.getAvgCurrentVal());
    setAvgBaselineVal(anomalyResult.getAvgBaselineVal());
    setFeedback(anomalyResult.getFeedback());
    setProperties(anomalyResult.getProperties());
  }

  public Multimap<String, String> getDimensionMap() {
    Multimap<String, String> dimMap = ArrayListMultimap.create();
    if (this.getMetricUrn() != null) {
      dimMap = MetricEntity.fromURN(this.getMetricUrn()).getFilters();
    }

    return dimMap;
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

  @Deprecated
  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  @Deprecated
  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }

  public Set<MergedAnomalyResultDTO> getChildren() {
    return children;
  }

  public void setChildren(Set<MergedAnomalyResultDTO> children) {
    this.children = children;
  }

  public boolean isRenotify() {
    return renotify;
  }

  public void setRenotify(boolean renotify) {
    this.renotify = renotify;
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
