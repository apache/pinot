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

package org.apache.pinot.thirdeye.detection.alert.scheme;

import java.util.Comparator;
import java.util.Properties;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.EntityGroupKeyContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertScheme.class);

  protected final DetectionAlertConfigDTO subsConfig;
  protected final DetectionAlertFilterResult result;

  public static final String PROP_TEMPLATE = "template";

  protected static final Comparator<AnomalyResult> COMPARATOR_DESC =
      (o1, o2) -> -1 * Long.compare(o1.getStartTime(), o2.getStartTime());

  public enum AlertTemplate {
    DEFAULT_EMAIL,
    ENTITY_GROUPBY_REPORT
  }

  public DetectionAlertScheme(DetectionAlertConfigDTO subsConfig, DetectionAlertFilterResult result) {
    this.subsConfig = subsConfig;
    this.result = result;
  }

  public abstract void run() throws Exception;

  public void destroy() {
    // do nothing
  }

  /**
   * Plug the appropriate template based on configuration.
   */
  public static BaseNotificationContent buildNotificationContent(Properties alertSchemeClientConfigs) {
    AlertTemplate template = AlertTemplate.DEFAULT_EMAIL;
    if (alertSchemeClientConfigs != null && alertSchemeClientConfigs.containsKey(PROP_TEMPLATE)) {
      template = AlertTemplate.valueOf(alertSchemeClientConfigs.get(PROP_TEMPLATE).toString());
    }

    BaseNotificationContent content;
    switch (template) {
      case DEFAULT_EMAIL:
        content = new MetricAnomaliesContent();
        break;

      case ENTITY_GROUPBY_REPORT:
        content =  new EntityGroupKeyContent();
        break;

      default:
        throw new IllegalArgumentException(String.format("Unknown email template '%s'", template));
    }

    LOG.info("Using " + content.getClass().getSimpleName() + " to render the template.");
    return content;
  }

  /**
   * Fail the alert task if unable to notify owner. However, in case of dimensions recipient alerter,
   * do not fail the alert if a subset of recipients are invalid.
   */
  void handleAlertFailure(int numOfAnomalies, Exception e) throws Exception {
    // Dimension recipients not enabled
    if (this.result.getResult().size() == 1) {
      throw e;
    } else {
      LOG.warn("Skipping! Found illegal arguments while sending {} anomalies for alert {}." + " Exception message: ",
          numOfAnomalies, this.subsConfig.getId(), e);
    }
  }

  protected BaseNotificationContent getNotificationContent(Properties alertSchemeClientConfigs) {
    return buildNotificationContent(alertSchemeClientConfigs);
  }
}
