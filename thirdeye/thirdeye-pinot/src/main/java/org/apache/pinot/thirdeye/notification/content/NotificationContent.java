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

package org.apache.pinot.thirdeye.notification.content;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;


/**
 * Defines the notification content interface.
 */
public interface NotificationContent {

  /**
   * Initialize the content formatter
   */
  void init(Properties properties, ThirdEyeAnomalyConfiguration configuration);

  /**
   * Generate the template dictionary from the list of anomaly results to render in the template
   */
  Map<String, Object> format(Collection<AnomalyResult> anomalies, ADContentFormatterContext context);

  /**
   * Retrieves the template file (.ftl)
   */
  String getTemplate();

  /**
   * Cleanup any temporary data
   */
  void cleanup();
}
