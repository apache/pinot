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

package org.apache.pinot.thirdeye.dashboard.resources.v2.pojo;

import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import java.util.Map;


/**
 * Container object for user dashboard summarized anomaly view
 */
public class AnomalySummary {
  private long id;
  private long start;
  private long end;
  private Map<String, String> dimensions;
  private double severity;
  private double current;
  private double baseline;
  private AnomalyFeedbackType feedback;
  private String comment;
  private String metricName;
  private String metricUrn;
  private long metricId;
  private String functionName;
  private String dataset;
  private AnomalyClassificationType classification;
  private AnomalyResultSource source;
  private long detectionConfigId;

  public AnomalySummary() {
    // left blank
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  public double getSeverity() {
    return severity;
  }

  public void setSeverity(double severity) {
    this.severity = severity;
  }

  public double getCurrent() {
    return current;
  }

  public void setCurrent(double current) {
    this.current = current;
  }

  public double getBaseline() {
    return baseline;
  }

  public void setBaseline(double baseline) {
    this.baseline = baseline;
  }

  public AnomalyFeedbackType getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedbackType feedback) {
    this.feedback = feedback;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getMetricUrn() {
    return metricUrn;
  }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public AnomalyClassificationType getClassification() {
    return classification;
  }

  public void setClassification(AnomalyClassificationType classification) {
    this.classification = classification;
  }

  public long getDetectionConfigId() {
    return detectionConfigId;
  }

  public void setDetectionConfigId(long detectionConfigId) {
    this.detectionConfigId = detectionConfigId;
  }

  public AnomalyResultSource getSource() {
    return source;
  }

  public void setSource(AnomalyResultSource source) {
    this.source = source;
  }
}
