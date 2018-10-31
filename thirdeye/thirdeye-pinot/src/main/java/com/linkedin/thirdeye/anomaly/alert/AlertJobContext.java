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

package com.linkedin.thirdeye.anomaly.alert;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertJobContext extends JobContext {

  private Long alertConfigId;
  private AlertConfigDTO alertConfigDTO;

  public Long getAlertConfigId() {
    return alertConfigId;
  }

  public void setAlertConfigId(Long alertConfigId) {
    this.alertConfigId = alertConfigId;
  }

  public AlertConfigDTO getAlertConfigDTO() {
    return alertConfigDTO;
  }

  public void setAlertConfigDTO(AlertConfigDTO alertConfigDTO) {
    this.alertConfigDTO = alertConfigDTO;
  }
}
