package org.apache.pinot.thirdeye.notification.formatter.channels;

import java.util.Properties;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;


/**
 * This generic class is responsible for formatting the contents across notification channels
 */
public abstract class AlertContentFormatter {
  protected static final String PROP_SUBJECT_STYLE = "subject";

  protected Properties alertClientConfig;
  protected DetectionAlertConfigDTO subsConfig;
  protected ThirdEyeAnomalyConfiguration teConfig;
  protected BaseNotificationContent notificationContent;

  public AlertContentFormatter(Properties alertClientConfig, BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig, DetectionAlertConfigDTO subsConfig) {
    this.alertClientConfig = alertClientConfig;
    this.teConfig = teConfig;
    this.notificationContent = content;
    this.subsConfig = subsConfig;

    notificationContent.init(alertClientConfig, teConfig);
  }

  /**
   * Plug the appropriate subject style based on configuration
   */
  AlertConfigBean.SubjectType getSubjectType(Properties alertSchemeClientConfigs) {
    AlertConfigBean.SubjectType subjectType;
    if (alertSchemeClientConfigs != null && alertSchemeClientConfigs.containsKey(PROP_SUBJECT_STYLE)) {
      subjectType = AlertConfigBean.SubjectType.valueOf(alertSchemeClientConfigs.get(PROP_SUBJECT_STYLE).toString());
    } else {
      // To support the legacy email subject configuration
      subjectType = this.subsConfig.getSubjectType();
    }

    return subjectType;
  }
}
