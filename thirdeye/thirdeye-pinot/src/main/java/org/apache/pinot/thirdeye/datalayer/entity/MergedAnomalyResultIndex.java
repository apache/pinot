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

package org.apache.pinot.thirdeye.datalayer.entity;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;


public class MergedAnomalyResultIndex extends AbstractIndexEntity {


  long functionId;
  long detectionConfigId;
  long anomalyFeedbackId;
  long metricId;
  long startTime;
  long endTime;
  String collection;
  String metric;
  DimensionMap dimensions;
  boolean notified;
  boolean child;

  public long getDetectionConfigId() {
    return detectionConfigId;
  }

  public void setDetectionConfigId(long detectionConfigId) {
    this.detectionConfigId = detectionConfigId;
  }

  public long getAnomalyFeedbackId() {
    return anomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(long anomalyFeedbackId) {
    this.anomalyFeedbackId = anomalyFeedbackId;
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long anomalyFunctionId) {
    this.functionId = anomalyFunctionId;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public boolean isNotified() {
    return notified;
  }

  public void setNotified(boolean notified) {
    this.notified = notified;
  }

  public boolean isChild() {
    return child;
  }

  public void setChild(boolean child) {
    this.child = child;
  }
}
