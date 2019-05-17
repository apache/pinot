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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.quartz.CronExpression;

import static org.apache.pinot.thirdeye.detection.yaml.YamlDetectionAlertConfigTranslator.*;


public class SubscriptionConfigValidator implements ConfigValidator<DetectionAlertConfigDTO> {

  private static final String PROP_CLASS_NAME = "className";

  /**
   * Perform validation on the alert config like verifying if all the required fields are set
   */
  @Override
  public void validateConfig(DetectionAlertConfigDTO alertConfig) throws IllegalArgumentException {
    Preconditions.checkNotNull(alertConfig);

    // Check for all the required fields in the alert
    Preconditions.checkArgument(!StringUtils.isEmpty(alertConfig.getName()), "Subscription group name field cannot be left empty.");
    Preconditions.checkArgument(!StringUtils.isEmpty(alertConfig.getApplication()), "Application field cannot be left empty");
    Preconditions.checkArgument(!StringUtils.isEmpty(alertConfig.getFrom()), "From address field cannot be left empty");

    // Empty subscription properties
    Preconditions.checkArgument((alertConfig.getProperties() != null
            && alertConfig.getProperties().get(PROP_CLASS_NAME) != null
            && StringUtils.isNotEmpty((alertConfig.getProperties().get(PROP_CLASS_NAME).toString()))),
        "'Type' field cannot be left empty.");

    // Application name should be valid
    Preconditions.checkArgument(!DAORegistry.getInstance().getApplicationDAO().findByName(alertConfig.getApplication()).isEmpty(),
        "Application name doesn't exist in our registry. Please use an existing application name. You may"
            + " search for registered applications from the ThirdEye dashboard or reach out to ask_thirdeye if you wish"
            + " to setup a new application.");

    // Cron Validator
    Preconditions.checkArgument(CronExpression.isValidExpression(alertConfig.getCronExpression()),
        "The subscription cron specified is incorrect. Please verify your cron expression using online cron"
            + " makers.");

    // At least one alertScheme is required
    Preconditions.checkArgument((alertConfig.getAlertSchemes() != null && !alertConfig.getAlertSchemes().isEmpty()),
        "Alert scheme cannot be left empty");

    // At least one recipient must be specified
    Map<String, Object> recipients = ConfigUtils.getMap(alertConfig.getProperties().get(PROP_RECIPIENTS));
    Preconditions.checkArgument((!recipients.isEmpty() && !ConfigUtils.getList(recipients.get("to")).isEmpty()),
        "Please specify at least one recipient in the notification group. If you wish to unsubscribe, set"
            + " active to false.");

    // Properties cannot be empty
    Preconditions.checkArgument((alertConfig.getProperties() != null && !alertConfig.getProperties().isEmpty()),
        "Alert properties cannot be left empty. Please specify the recipients, subscribed detections, and"
            + " type.");

    // detectionConfigIds cannot be empty
    List<Long> detectionIds = ConfigUtils.getLongs(alertConfig.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    Preconditions.checkArgument((detectionIds != null && !detectionIds.isEmpty()),
        "A notification group should subscribe to at least one alert. If you wish to unsubscribe, set active"
            + " to false.");

    // TODO add more checks like email validity, alert type check, scheme type check etc.
  }

  // Validate the raw subscription yaml configuration
  @Override
  public void validateConfig(Map<String, Object> config) throws IllegalArgumentException {
    // Add validations
  }

  /**
   * Perform validation on the updated alert config. Check for fields which shouldn't be
   * updated by the user.
   */
  @Override
  public void validateUpdatedConfig(DetectionAlertConfigDTO updatedAlertConfig, DetectionAlertConfigDTO oldAlertConfig)
      throws IllegalArgumentException {
    validateConfig(updatedAlertConfig);
    Preconditions.checkNotNull(oldAlertConfig);

    Preconditions.checkArgument(updatedAlertConfig.getId().equals(oldAlertConfig.getId()));
    Long newHighWatermark = updatedAlertConfig.getHighWaterMark();
    Long oldHighWatermark = oldAlertConfig.getHighWaterMark();
    if (newHighWatermark != null && oldHighWatermark != null && newHighWatermark.longValue() != oldHighWatermark) {
      throw new IllegalArgumentException("HighWaterMark has been modified. This is not allowed");
    }
    if (updatedAlertConfig.getVectorClocks() != null) {
      for (Map.Entry<Long, Long> vectorClock : updatedAlertConfig.getVectorClocks().entrySet()) {
        if (!oldAlertConfig.getVectorClocks().containsKey(vectorClock.getKey())
            || oldAlertConfig.getVectorClocks().get(vectorClock.getKey()).longValue() != vectorClock.getValue()) {
          throw new IllegalArgumentException("Vector clock has been modified. This is not allowed.");
        }
      }
    }
  }
}
