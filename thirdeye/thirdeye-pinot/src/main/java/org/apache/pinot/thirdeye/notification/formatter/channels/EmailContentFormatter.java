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

package org.apache.pinot.thirdeye.notification.formatter.channels;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.mail.HtmlEmail;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class formats the content for email alerts.
 */
public class EmailContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(EmailContentFormatter.class);

  private static final String CHARSET = "UTF-8";

  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfig;
  private BaseNotificationContent notificationContent;

  public EmailContentFormatter(BaseNotificationContent formatter, ThirdEyeAnomalyConfiguration config) {
    this(formatter, new Properties(), config);
  }

  public EmailContentFormatter(BaseNotificationContent formatter, Properties emailProps, ThirdEyeAnomalyConfiguration teConfig) {
    init(formatter, teConfig, emailProps);
  }

  private void init(BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig,
      Properties emailProps) {
    this.notificationContent = content;
    this.thirdEyeAnomalyConfig = teConfig;

    content.init(emailProps, teConfig);
  }

  public EmailEntity getEmailEntity(DetectionAlertFilterRecipients recipients, String subject,
      Collection<AnomalyResult> anomalies, ADContentFormatterContext context) {
    Map<String, Object> templateData = notificationContent.format(anomalies, context);
    templateData.put("dashboardHost", thirdEyeAnomalyConfig.getDashboardHost());

    String outputSubject = notificationContent.makeSubject(subject, context.getNotificationConfig().getSubjectType(), templateData);
    return buildEmailEntity(templateData, outputSubject, recipients, context.getNotificationConfig().getFrom(), notificationContent.getTemplate());
  }

  /**
   * Apply the parameter map to given email template, and format it as EmailEntity
   */
  private EmailEntity buildEmailEntity(Map<String, Object> paramMap, String subject,
      DetectionAlertFilterRecipients recipients, String fromEmail, String emailTemplate) {
    if (Strings.isNullOrEmpty(fromEmail)) {
      throw new IllegalArgumentException("Invalid sender's email");
    }

    HtmlEmail email = new HtmlEmail();
    String cid = "";
    try {
      if (StringUtils.isNotBlank(notificationContent.getSnaphotPath())) {
        cid = email.embed(new File(notificationContent.getSnaphotPath()));
      }
    } catch (Exception e) {
      LOG.error("Exception while embedding screenshot for anomaly", e);
    }
    paramMap.put("cid", cid);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EmailEntity emailEntity = new EmailEntity();
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/org/apache/pinot/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(emailTemplate);

      template.process(paramMap, out);

      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);

      emailEntity.setFrom(fromEmail);
      emailEntity.setTo(recipients);
      emailEntity.setSubject(subject);
      email.setHtmlMsg(alertEmailHtml);
      emailEntity.setContent(email);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return emailEntity;
  }
}
