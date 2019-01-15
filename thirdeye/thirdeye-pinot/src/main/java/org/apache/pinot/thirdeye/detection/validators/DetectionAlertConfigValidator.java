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

package org.apache.pinot.thirdeye.detection.validators;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.junit.Assert;

import static org.apache.pinot.thirdeye.detection.yaml.YamlDetectionAlertConfigTranslator.*;


public class DetectionAlertConfigValidator extends ConfigValidator {

  private static final DetectionAlertConfigValidator INSTANCE = new DetectionAlertConfigValidator();
  private static final String PROP_CLASS_NAME = "className";

  public static DetectionAlertConfigValidator getInstance() {
    return INSTANCE;
  }

  /**
   * Perform validation on the alert config like verifying if all the required fields are set
   */
  @SuppressWarnings("unchecked")
  public boolean validateConfig(DetectionAlertConfigDTO alertConfig,  Map<String, String> responseMessage) {
    boolean isValid = true;

    // Check for all the required fields in the alert
    if (StringUtils.isEmpty(alertConfig.getName())) {
      responseMessage.put("message", "Subscription group name field cannot be left empty.");
      return false;
    }
    if (StringUtils.isEmpty(alertConfig.getApplication())) {
      responseMessage.put("message", "Application field cannot be left empty");
      return false;
    }
    if (StringUtils.isEmpty(alertConfig.getFrom())) {
      responseMessage.put("message", "From address field cannot be left empty");
      return false;
    }
    if (alertConfig.getProperties() == null || alertConfig.getProperties().get(PROP_CLASS_NAME) == null
        || StringUtils.isEmpty(alertConfig.getProperties().get(PROP_CLASS_NAME).toString())) {
      responseMessage.put("message", "'Type' field cannot be left empty.");
      return false;
    }
    // At least one alertScheme is required
    if (alertConfig.getAlertSchemes() == null || alertConfig.getAlertSchemes().size() == 0) {
      responseMessage.put("message", "Alert scheme cannot be left empty");
      return false;
    }
    // Properties cannot be empty
    if (alertConfig.getProperties() == null || alertConfig.getProperties().isEmpty()) {
      responseMessage.put("message", "Alert properties cannot be left empty. Please specify the recipients,"
          + " subscribed detections, and type.");
      return false;
    }
    // detectionConfigIds cannot be empty
    List<Long> detectionIds = (List<Long>) alertConfig.getProperties().get(PROP_DETECTION_CONFIG_IDS);
    if (detectionIds == null || detectionIds.isEmpty()) {
      responseMessage.put("message", "A notification group should subscribe to at least one alert. If you wish to"
          + " unsubscribe, set active to false.");
      return false;
    }
    // At least one recipient must be specified
    Map<String, Object> recipients = (Map<String, Object>) alertConfig.getProperties().get(PROP_RECIPIENTS);
    if (recipients == null || recipients.isEmpty() || ((List<String>) recipients.get("to")).isEmpty()) {
      responseMessage.put("message", "Please specify at least one recipient in the notification group. If you wish to"
          + " unsubscribe, set active to false.");
      return false;
    }
    // Application name should be valid
    if (DAORegistry.getInstance().getApplicationDAO().findByName(alertConfig.getApplication()).size() == 0) {
      responseMessage.put("message", "Application name doesn't exist in our registry. Please use an existing"
          + " application name. You may search for registered applications from the ThirdEye dashboard or reach out"
          + " to ask_thirdeye if you wish to setup a new application.");
      return false;
    }

    // TODO add more checks like cron validity, email validity, alert type check, scheme type check etc.

    return isValid;
  }

  /**
   * Perform validation on the updated alert config. Check for fields which shouldn't be
   * updated by the user.
   */
  @SuppressWarnings("unchecked")
  public boolean validateUpdatedConfig(DetectionAlertConfigDTO updatedAlertConfig,
      DetectionAlertConfigDTO oldAlertConfig, Map<String, String> responseMessage) {
    boolean isValid = true;

    if (!validateConfig(updatedAlertConfig, responseMessage)) {
      isValid = false;
    }

    Long newHighWatermark = updatedAlertConfig.getHighWaterMark();
    Long oldHighWatermark = oldAlertConfig.getHighWaterMark();
    if (newHighWatermark != null && oldHighWatermark != null && newHighWatermark.longValue() != oldHighWatermark) {
      responseMessage.put("message", "HighWaterMark has been modified. This is not allowed");
      isValid = false;
    }
    if (updatedAlertConfig.getVectorClocks() != null) {
      for (Map.Entry<Long, Long> vectorClock : updatedAlertConfig.getVectorClocks().entrySet()) {
        if (!oldAlertConfig.getVectorClocks().containsKey(vectorClock.getKey())
            || oldAlertConfig.getVectorClocks().get(vectorClock.getKey()).longValue() != vectorClock.getValue()) {
          responseMessage.put("message", "Vector clock has been modified. This is not allowed.");
          isValid = false;
        }
      }
    }

    return isValid;
  }
}
