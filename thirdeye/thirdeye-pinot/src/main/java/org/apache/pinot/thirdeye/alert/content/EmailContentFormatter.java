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

package org.apache.pinot.thirdeye.alert.content;

import org.apache.pinot.thirdeye.alert.commons.EmailEntity;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import java.util.Collection;
import java.util.Properties;


/**
 * The module actually put data into emails. The responsibility of this interface is:
 *  - convert anomalies information into instances known by email templates
 *  - properly format alert emails
 *  - acquire auxiliary information from outside
 */
public interface EmailContentFormatter {
  /**
   * Initialize the email content formatter
   * @param properties
   * @param configuration
   */
  void init(Properties properties, EmailContentFormatterConfiguration configuration);

  /**
   * Get the email entity for the list of anomaly results.
   * @param alertConfigDTO
   * @param recipients
   * @param subject
   * @param groupId
   * @param groupName
   * @param anomalies
   * @return
   */
  EmailEntity getEmailEntity(AlertConfigDTO alertConfigDTO, DetectionAlertFilterRecipients recipients, String subject,
      Long groupId, String groupName, Collection<AnomalyResult> anomalies, EmailContentFormatterContext context);

  /**
   * Cleanup any temporary data
   */
  void cleanup();
}
