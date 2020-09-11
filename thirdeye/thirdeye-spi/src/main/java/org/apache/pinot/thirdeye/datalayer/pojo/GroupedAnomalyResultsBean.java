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

package org.apache.pinot.thirdeye.datalayer.pojo;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import java.util.ArrayList;
import java.util.List;

public class GroupedAnomalyResultsBean extends AbstractBean {
  private long alertConfigId;
  private DimensionMap dimensions = new DimensionMap();
  private List<Long> anomalyResultsId = new ArrayList<>();
  // the max endTime among all its merged anomaly results
  private long endTime;
  private boolean isNotified = false;

  public long getAlertConfigId() {
    return alertConfigId;
  }

  public void setAlertConfigId(long alertConfigId) {
    this.alertConfigId = alertConfigId;
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
  }

  public List<Long> getAnomalyResultsId() {
    return anomalyResultsId;
  }

  public void setAnomalyResultsId(List<Long> anomalyResultsId) {
    this.anomalyResultsId = anomalyResultsId;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public boolean isNotified() {
    return isNotified;
  }

  public void setNotified(boolean notified) {
    isNotified = notified;
  }
}
