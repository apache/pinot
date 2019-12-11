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

package org.apache.pinot.thirdeye.detection.alert.filter;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;


public class SubscriptionUtils {

  /**
   * Make a new(child) subscription group from given(parent) subscription group.
   *
   * This is used   preparing the notification for each dimensional recipients.
   */
  public static DetectionAlertConfigDTO makeChildSubscriptionConfig(
      DetectionAlertConfigDTO parentConfig,
      Map<String, Object> alertSchemes,
      Map<String, String> refLinks) {
    // TODO: clone object using serialization rather than manual copy
    DetectionAlertConfigDTO subsConfig = new DetectionAlertConfigDTO();
    subsConfig.setId(parentConfig.getId());
    subsConfig.setFrom(parentConfig.getFrom());
    subsConfig.setActive(parentConfig.isActive());
    subsConfig.setOwners(parentConfig.getOwners());
    subsConfig.setYaml(parentConfig.getYaml());
    subsConfig.setApplication(parentConfig.getApplication());
    subsConfig.setName(parentConfig.getName());
    subsConfig.setSubjectType(parentConfig.getSubjectType());
    subsConfig.setProperties(parentConfig.getProperties());
    subsConfig.setVectorClocks(parentConfig.getVectorClocks());
    subsConfig.setHighWaterMark(parentConfig.getHighWaterMark());

    subsConfig.setAlertSchemes(alertSchemes);
    subsConfig.setReferenceLinks(refLinks);

    return subsConfig;
  }
}
