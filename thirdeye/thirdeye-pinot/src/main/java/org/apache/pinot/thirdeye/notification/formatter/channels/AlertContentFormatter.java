package org.apache.pinot.thirdeye.notification.formatter.channels;

import java.util.Properties;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;


public abstract class AlertContentFormatter {
  protected static final String PROP_SUBJECT_STYLE = "subject";

  protected Properties alertClientConfig;
  protected ADContentFormatterContext adContext;
  protected ThirdEyeAnomalyConfiguration teConfig;
  protected BaseNotificationContent notificationContent;

  public AlertContentFormatter(Properties alertClientConfig, BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig, ADContentFormatterContext adContext) {
    this.alertClientConfig = alertClientConfig;
    this.teConfig = teConfig;
    this.notificationContent = content;
    this.adContext = adContext;

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
      subjectType = this.adContext.getNotificationConfig().getSubjectType();
    }

    return subjectType;
  }
}
