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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * Container class for notification properties
 */
public class DetectionAlertFilterNotification {

  Map<String, Object> notificationSchemeProps;
  Multimap<String, String> dimensionFilters;

  public DetectionAlertFilterNotification(Map<String, Object> notificationSchemeProps) {
    this(notificationSchemeProps, ArrayListMultimap.create());
  }

  public DetectionAlertFilterNotification(Map<String, Object> notificationSchemeProps, Multimap<String, String> dimensionFilters) {
    this.notificationSchemeProps = notificationSchemeProps;
    this.dimensionFilters = dimensionFilters;
  }

  public Map<String, Object> getNotificationSchemeProps() {
    return notificationSchemeProps;
  }

  public void setNotificationSchemeProps(Map<String, Object> notificationSchemeProps) {
    this.notificationSchemeProps = notificationSchemeProps;
  }

  public Multimap<String, String> getDimensionFilters() {
    return dimensionFilters;
  }

  public void setDimensionFilters(Multimap<String, String> dimensions) {
    this.dimensionFilters = dimensions;
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
    return Objects.equals(notificationSchemeProps, that.notificationSchemeProps) &&
        Objects.equals(dimensionFilters, that.dimensionFilters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(notificationSchemeProps, dimensionFilters);
  }

}
