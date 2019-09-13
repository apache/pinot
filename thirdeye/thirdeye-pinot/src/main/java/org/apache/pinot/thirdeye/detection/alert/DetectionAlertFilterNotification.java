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

package org.apache.pinot.thirdeye.detection.alert;

import java.util.Map;
import java.util.Objects;


/**
 * Container class for notification properties
 */
public class DetectionAlertFilterNotification {
  Map<String, Object> notificationProps;

  public DetectionAlertFilterNotification(Map<String, Object> notificationProps) {
    this.notificationProps = notificationProps;
  }

  public Map<String, Object> getNotificationProps() {
    return notificationProps;
  }

  public DetectionAlertFilterNotification setNotificationProps(Map<String, Object> notificationProps) {
    this.notificationProps = notificationProps;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionAlertFilterNotification that = (DetectionAlertFilterNotification) o;
    return Objects.equals(notificationProps, that.notificationProps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(notificationProps);
  }

}
