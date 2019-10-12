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

import com.google.common.base.Throwables;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
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
import org.apache.pinot.thirdeye.notification.content.templates.EntityGroupKeyContent;
import org.apache.pinot.thirdeye.notification.content.templates.HierarchicalAnomaliesContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class formats the content for email alerts.
 */
public class EmailContentFormatter extends AlertContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(EmailContentFormatter.class);

  private static final String CHARSET = "UTF-8";

  private static final Map<String, String> alertContentToTemplateMap;
  static {
    Map<String, String> aMap = new HashMap<>();
    aMap.put(MetricAnomaliesContent.class.getSimpleName(), "metric-anomalies-template.ftl");
    aMap.put(EntityGroupKeyContent.class.getSimpleName(), "entity-groupkey-anomaly-report.ftl");
    aMap.put(HierarchicalAnomaliesContent.class.getSimpleName(), "hierarchical-anomalies-email-template.ftl");
    alertContentToTemplateMap = Collections.unmodifiableMap(aMap);
  }

  public EmailContentFormatter(Properties emailClientConfig, BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig, ADContentFormatterContext adContext) {
    super(emailClientConfig, content, teConfig, adContext);
  }

  public EmailEntity getEmailEntity(Collection<AnomalyResult> anomalies) {
    Map<String, Object> templateData = notificationContent.format(anomalies, adContext);
    templateData.put("dashboardHost", teConfig.getDashboardHost());
    return buildEmailEntity(alertContentToTemplateMap.get(notificationContent.getTemplate()), templateData);
  }

  /**
   * Apply the parameter map to given email template, and format it as EmailEntity
   */
  private EmailEntity buildEmailEntity(String emailTemplate, Map<String, Object> templateValues) {
    HtmlEmail email = new HtmlEmail();
    String cid = "";
    try {
      if (StringUtils.isNotBlank(notificationContent.getSnaphotPath())) {
        cid = email.embed(new File(notificationContent.getSnaphotPath()));
      }
    } catch (Exception e) {
      LOG.error("Exception while embedding screenshot for anomaly", e);
    }
    templateValues.put("cid", cid);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EmailEntity emailEntity = new EmailEntity();
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/org/apache/pinot/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(emailTemplate);

      template.process(templateValues, out);

      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);

      String subject = BaseNotificationContent.makeSubject(super.getSubjectType(alertClientConfig), adContext.getNotificationConfig(), templateValues);
      emailEntity.setSubject(subject);
      email.setHtmlMsg(alertEmailHtml);
      emailEntity.setContent(email);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return emailEntity;
  }
}
