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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;

@Deprecated
public class RawAnomalyResultDTO extends RawAnomalyResultBean {

  private AnomalyFeedbackDTO feedback;

  @JsonIgnore
  private AnomalyFunctionDTO function;

  public RawAnomalyResultDTO() {
    super();
  }

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }

  // TODO: rename to getMetric and update the column name in DB?
  public String getMetric() {
    return function.getTopicMetric();
  }

  public String getCollection() {
    return function.getCollection();
  }

  public AnomalyFeedbackDTO getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedbackDTO feedback) {
    this.feedback = feedback;
  }

}
