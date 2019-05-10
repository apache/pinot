/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import java.util.Objects;


/**
 * The class for evaluation metrics.
 */
public class EvaluationBean extends AbstractBean {
  private long detectionConfigId; // the detection config id
  private long startTime; // the start time for the detection window being monitored
  private long endTime; // the end time for the detection window being monitored
  private String detectorName; // the name for the detector
  private double mape; //  the mean absolute percentage error (MAPE)

  public long getDetectionConfigId() {
    return detectionConfigId;
  }

  public void setDetectionConfigId(long detectionConfigId) {
    this.detectionConfigId = detectionConfigId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getDetectorName() {
    return detectorName;
  }

  public void setDetectorName(String detectorName) {
    this.detectorName = detectorName;
  }

  public double getMape() {
    return mape;
  }

  public void setMape(double mape) {
    this.mape = mape;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EvaluationBean)) {
      return false;
    }
    EvaluationBean that = (EvaluationBean) o;
    return detectionConfigId == that.detectionConfigId && startTime == that.startTime && endTime == that.endTime
        && Double.compare(that.mape, mape) == 0 && Objects.equals(detectorName, that.detectorName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(detectionConfigId, startTime, endTime, detectorName, mape);
  }

  @Override
  public String toString() {
    return "EvaluationBean{" + "detectionConfigId=" + detectionConfigId + ", startTime=" + startTime + ", endTime="
        + endTime + ", detectorName='" + detectorName + '\'' + ", mape=" + mape + '}';
  }
}
