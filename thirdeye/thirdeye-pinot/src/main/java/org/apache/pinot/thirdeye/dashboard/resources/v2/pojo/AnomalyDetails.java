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

import java.util.List;

import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;

public class AnomalyDetails {

  // anomaly details
  private Long anomalyId;
  private String metric;
  private Long metricId;
  private String dataset;
  private String timeUnit;
  private String externalUrl;

  List<String> dates;
  private String currentEnd;
  private String currentStart;
  private List<String> baselineValues;
  private List<String> currentValues;
  private String current = "1000";
  private String baseline = "2000";

  //function details
  private String anomalyStart; // start and end of anomaly
  private String anomalyEnd;
  private String anomalyRegionStart; // start and end of region to highlight as anomaly
  private String anomalyRegionEnd;
  private Long anomalyFunctionId = 5L;
  private String anomalyFunctionName;
  private String anomalyFunctionType;
  private String anomalyFunctionProps;
  private String anomalyFunctionDimension;
  private String anomalyFeedback;
  private String anomalyFeedbackStatus;
  private String anomalyFeedbackComments;
  private String anomalyResultSource;
  private String issueType;

  public Long getAnomalyId() {
    return anomalyId;
  }

  public Long getMetricId() {
    return metricId;
  }

  public void setMetricId(Long metricId) {
    this.metricId = metricId;
  }

  public void setAnomalyId(Long anomalyId) {
    this.anomalyId = anomalyId;
  }

  public String getExternalUrl() {
    return externalUrl;
  }

  public void setExternalUrl(String externalUrl) {
    this.externalUrl = externalUrl;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(String timeUnit) {
    this.timeUnit = timeUnit;
  }

  public List<String> getDates() {
    return dates;
  }

  public void setDates(List<String> dates) {
    this.dates = dates;
  }

  public String getCurrentEnd() {
    return currentEnd;
  }

  public void setCurrentEnd(String currentEnd) {
    this.currentEnd = currentEnd;
  }

  public String getCurrentStart() {
    return currentStart;
  }

  public void setCurrentStart(String currentStart) {
    this.currentStart = currentStart;
  }

  public List<String> getBaselineValues() {
    return baselineValues;
  }

  public void setBaselineValues(List<String> baselineValues) {
    this.baselineValues = baselineValues;
  }

  public List<String> getCurrentValues() {
    return currentValues;
  }

  public void setCurrentValues(List<String> currentValues) {
    this.currentValues = currentValues;
  }

  public String getCurrent() {
    return current;
  }

  public void setCurrent(String current) {
    this.current = current;
  }

  public String getBaseline() {
    return baseline;
  }

  public void setBaseline(String baseline) {
    this.baseline = baseline;
  }

  public String getAnomalyStart() {
    return anomalyStart;
  }

  public void setAnomalyStart(String anomalyStart) {
    this.anomalyStart = anomalyStart;
  }

  public String getAnomalyEnd() {
    return anomalyEnd;
  }

  public void setAnomalyEnd(String anomalyEnd) {
    this.anomalyEnd = anomalyEnd;
  }

  public String getAnomalyRegionStart() {
    return anomalyRegionStart;
  }

  public void setAnomalyRegionStart(String anomalyRegionStart) {
    this.anomalyRegionStart = anomalyRegionStart;
  }

  public String getAnomalyRegionEnd() {
    return anomalyRegionEnd;
  }

  public void setAnomalyRegionEnd(String anomalyRegionEnd) {
    this.anomalyRegionEnd = anomalyRegionEnd;
  }

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public String getAnomalyFunctionName() {
    return anomalyFunctionName;
  }

  public void setAnomalyFunctionName(String anomalyFunctionName) {
    this.anomalyFunctionName = anomalyFunctionName;
  }

  public String getAnomalyFunctionType() {
    return anomalyFunctionType;
  }

  public void setAnomalyFunctionType(String anomalyFunctionType) {
    this.anomalyFunctionType = anomalyFunctionType;
  }

  public String getAnomalyFunctionProps() {
    return anomalyFunctionProps;
  }

  public void setAnomalyFunctionProps(String anomalyFunctionProps) {
    this.anomalyFunctionProps = anomalyFunctionProps;
  }

  public String getAnomalyFunctionDimension() {
    return anomalyFunctionDimension;
  }

  public void setAnomalyFunctionDimension(String anomalyFunctionDimension) {
    this.anomalyFunctionDimension = anomalyFunctionDimension;
  }

  public String getAnomalyFeedback() {
    return anomalyFeedback;
  }

  public void setAnomalyFeedback(String anomalyFeedback) {
    this.anomalyFeedback = anomalyFeedback;
  }


  public String getAnomalyFeedbackStatus() {
    return anomalyFeedbackStatus;
  }

  public void setAnomalyFeedbackStatus(String anomalyFeedbackStatus) {
    this.anomalyFeedbackStatus = anomalyFeedbackStatus;
  }

  public String getAnomalyFeedbackComments() {
    return anomalyFeedbackComments;
  }

  public void setAnomalyFeedbackComments(String anomalyFeedbackComments) {
    this.anomalyFeedbackComments = anomalyFeedbackComments;
  }

  // TODO remove this unnecessary translation. Warning - this is not trivial. It extensively touches frontend and search filters
  public static String getFeedbackStringFromFeedbackType(AnomalyFeedbackType feedbackType) {
    switch (feedbackType) {
      case ANOMALY:
        return "Confirmed Anomaly";
      case ANOMALY_NEW_TREND:
        return "Confirmed - New Trend";
      case NOT_ANOMALY:
        return "False Alarm";
      case ANOMALY_EXPECTED:
        return "Expected Anomaly";
      case NO_FEEDBACK:
        return "Not Resolved";
      default:
        return "(unknown)";
    }
  }

  public String getIssueType() {
    return issueType;
  }

  public void setIssueType(String issueType) {
    this.issueType = issueType;
  }

  public String getAnomalyResultSource() {
    return anomalyResultSource;
  }

  public void setAnomalyResultSource(String anomalyResultSource) {
    this.anomalyResultSource = anomalyResultSource;
  }
}
