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

package com.linkedin.thirdeye.tools.anomaly.report;

import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;


public class AnomalyReportConfig {
  private String startTimeIso;
  private String endTimeIso;
  private String thirdEyeConfigDirectoryPath;
  private String datasets;
  private String teBaseUrl;
  private DetectionAlertFilterRecipients emailRecipients;
  private boolean includeNotifiedOnly = true;

  public String getEndTimeIso() {
    return endTimeIso;
  }

  public String getTeBaseUrl() {
    return teBaseUrl;
  }

  public void setTeBaseUrl(String teBaseUrl) {
    this.teBaseUrl = teBaseUrl;
  }

  public void setEndTimeIso(String endTimeIso) {
    this.endTimeIso = endTimeIso;
  }

  public String getStartTimeIso() {
    return startTimeIso;
  }

  public void setStartTimeIso(String startTimeIso) {
    this.startTimeIso = startTimeIso;
  }

  public String getDatasets() {
    return datasets;
  }

  public void setDatasets(String datasets) {
    this.datasets = datasets;
  }

  public String getThirdEyeConfigDirectoryPath() {
    return thirdEyeConfigDirectoryPath;
  }

  public void setThirdEyeConfigDirectoryPath(String thirdEyeConfigDirectoryPath) {
    this.thirdEyeConfigDirectoryPath = thirdEyeConfigDirectoryPath;
  }

  public DetectionAlertFilterRecipients getEmailRecipients() {
    return emailRecipients;
  }

  public void setEmailRecipients(DetectionAlertFilterRecipients emailRecipients) {
    this.emailRecipients = emailRecipients;
  }

  public boolean isIncludeNotifiedOnly() {
    return includeNotifiedOnly;
  }

  public void setIncludeNotifiedOnly(boolean includeNotifiedOnly) {
    this.includeNotifiedOnly = includeNotifiedOnly;
  }
}
