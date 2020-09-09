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

import java.util.Objects;

import com.google.common.base.MoreObjects;


/**
 * Bean to store the detection status
 */
public class DetectionStatusBean extends AbstractBean implements Comparable<DetectionStatusBean>{

  private long functionId;
  private String dataset;
  private long dateToCheckInMS;
  private String dateToCheckInSDF;
  private boolean detectionRun = false;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public long getDateToCheckInMS() {
    return dateToCheckInMS;
  }

  public void setDateToCheckInMS(long dateToCheckInMS) {
    this.dateToCheckInMS = dateToCheckInMS;
  }

  public String getDateToCheckInSDF() {
    return dateToCheckInSDF;
  }

  public void setDateToCheckInSDF(String dateToCheckInSDF) {
    this.dateToCheckInSDF = dateToCheckInSDF;
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  public boolean isDetectionRun() {
    return detectionRun;
  }

  public void setDetectionRun(boolean detectionRun) {
    this.detectionRun = detectionRun;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DetectionStatusBean)) {
      return false;
    }
    DetectionStatusBean ds = (DetectionStatusBean) o;
    return Objects.equals(getId(), ds.getId()) && Objects.equals(dataset, ds.getDataset())
        && Objects.equals(dateToCheckInMS, ds.getDateToCheckInMS())
        && Objects.equals(dateToCheckInSDF, ds.getDateToCheckInSDF())
        && Objects.equals(functionId, ds.getFunctionId()) && Objects.equals(detectionRun, ds.isDetectionRun());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, dateToCheckInMS, dateToCheckInSDF, functionId, detectionRun);
  }

  @Override
  public int compareTo(DetectionStatusBean o) {
    return dateToCheckInSDF.compareTo(o.getDateToCheckInSDF());
  }
}
