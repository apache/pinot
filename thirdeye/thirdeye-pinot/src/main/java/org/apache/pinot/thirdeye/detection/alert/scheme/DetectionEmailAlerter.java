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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.AlertUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.annotation.AlertScheme;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.formatter.channels.EmailContentFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration.SMTP_CONFIG_KEY;


/**
 * This class is responsible for sending the email alerts
 */
@AlertScheme(type = "EMAIL")
public class DetectionEmailAlerter extends DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionEmailAlerter.class);

  public static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";

  private static final String PROP_EMAIL_WHITELIST = "emailWhitelist";
  private static final String PROP_ADMIN_RECIPIENTS = "adminRecipients";
  private static final String PROP_FROM_ADDRESS = "fromAddress";

  public static final String PROP_EMAIL_SCHEME = "emailScheme";

  private List<String> emailBlacklist = new ArrayList<>(Arrays.asList("me@company.com", "cc_email@company.com"));
  private static final Comparator<AnomalyResult> COMPARATOR_DESC =
      (o1, o2) -> -1 * Long.compare(o1.getStartTime(), o2.getStartTime());

  private ThirdEyeAnomalyConfiguration teConfig;
  private SmtpConfiguration smtpConfig;

  public DetectionEmailAlerter(DetectionAlertConfigDTO subsConfig, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) {
    super(subsConfig, result);
    this.teConfig = thirdeyeConfig;
    this.smtpConfig = SmtpConfiguration.createFromProperties(this.teConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY));
  }

  private Set<String> retainWhitelisted(Set<String> recipients, Collection<String> emailWhitelist) {
    if (recipients != null) {
      recipients.retainAll(emailWhitelist);
    }
    return recipients;
  }

  private Set<String> removeBlacklisted(Set<String> recipients, Collection<String> emailBlacklist) {
    if (recipients != null) {
      recipients.removeAll(emailBlacklist);
    }
    return recipients;
  }

  private void configureAdminRecipients(DetectionAlertFilterRecipients recipients) {
    if (recipients.getCc() == null) {
      recipients.setCc(new HashSet<>());
    }
    recipients.getCc().addAll(ConfigUtils.getList(this.teConfig.getAlerterConfiguration()
        .get(SMTP_CONFIG_KEY).get(PROP_ADMIN_RECIPIENTS)));
  }

  private void whitelistRecipients(DetectionAlertFilterRecipients recipients) {
    if (recipients != null) {
      List<String> emailWhitelist = ConfigUtils.getList(
          this.teConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY).get(PROP_EMAIL_WHITELIST));
      if (!emailWhitelist.isEmpty()) {
        recipients.setTo(retainWhitelisted(recipients.getTo(), emailWhitelist));
        recipients.setCc(retainWhitelisted(recipients.getCc(), emailWhitelist));
        recipients.setBcc(retainWhitelisted(recipients.getBcc(), emailWhitelist));
      }
    }
  }

  private void blacklistRecipients(DetectionAlertFilterRecipients recipients) {
    if (recipients != null && !emailBlacklist.isEmpty()) {
      recipients.setTo(removeBlacklisted(recipients.getTo(), emailBlacklist));
      recipients.setCc(removeBlacklisted(recipients.getCc(), emailBlacklist));
      recipients.setBcc(removeBlacklisted(recipients.getBcc(), emailBlacklist));
    }
  }

  private void validateAlert(DetectionAlertFilterRecipients recipients, List<AnomalyResult> anomalies) {
    Preconditions.checkNotNull(recipients);
    Preconditions.checkNotNull(anomalies);
    if (recipients.getTo() == null || recipients.getTo().isEmpty()) {
      throw new IllegalArgumentException("Email doesn't have any valid (whitelisted) recipients.");
    }
    if (anomalies.size() == 0) {
      throw new IllegalArgumentException("Zero anomalies found");
    }
  }

  private HtmlEmail prepareEmailContent(DetectionAlertConfigDTO subsConfig, Properties emailClientConfigs,
      List<AnomalyResult> anomalies, DetectionAlertFilterRecipients recipients) throws Exception {
    configureAdminRecipients(recipients);
    whitelistRecipients(recipients);
    blacklistRecipients(recipients);
    validateAlert(recipients, anomalies);

    BaseNotificationContent content = getNotificationContent(emailClientConfigs);
    EmailEntity emailEntity = new EmailContentFormatter(emailClientConfigs, content, this.teConfig, subsConfig)
        .getEmailEntity(anomalies);
    if (Strings.isNullOrEmpty(this.subsConfig.getFrom())) {
      String fromAddress = MapUtils.getString(this.teConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY), PROP_FROM_ADDRESS);
      if (Strings.isNullOrEmpty(fromAddress)) {
        throw new IllegalArgumentException("Invalid sender's email");
      }
      this.subsConfig.setFrom(fromAddress);
    }

    HtmlEmail email = emailEntity.getContent();
    email.setSubject(emailEntity.getSubject());
    email.setFrom(this.subsConfig.getFrom());
    email.setTo(AlertUtils.toAddress(recipients.getTo()));
    if (!CollectionUtils.isEmpty(recipients.getCc())) {
      email.setCc(AlertUtils.toAddress(recipients.getCc()));
    }
    if (!CollectionUtils.isEmpty(recipients.getBcc())) {
      email.setBcc(AlertUtils.toAddress(recipients.getBcc()));
    }

    return getHtmlContent(emailEntity);
  }

  protected HtmlEmail getHtmlContent(EmailEntity emailEntity) {
    return emailEntity.getContent();
  }

  /** Sends email according to the provided config. */
  private void sendEmail(HtmlEmail email) throws EmailException {
    SmtpConfiguration config = this.smtpConfig;
    email.setHostName(config.getSmtpHost());
    email.setSmtpPort(config.getSmtpPort());
    if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
      email.setAuthenticator(new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
      email.setSSLOnConnect(true);
      email.setSslSmtpPort(Integer.toString(config.getSmtpPort()));
    }
    email.send();

    int recipientCount = email.getToAddresses().size() + email.getCcAddresses().size() + email.getBccAddresses().size();
    LOG.info("Email sent with subject '{}' to {} recipients", email.getSubject(), recipientCount);
  }

  private void generateAndSendEmails(DetectionAlertFilterResult results) throws Exception {
    LOG.info("Preparing an email alert for subscription group id {}", this.subsConfig.getId());
    Preconditions.checkNotNull(results.getResult());
    for (Map.Entry<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> result : results.getResult().entrySet()) {
      try {
        DetectionAlertConfigDTO subsConfig = result.getKey().getSubscriptionConfig();
        if (subsConfig.getAlertSchemes().get(PROP_EMAIL_SCHEME) == null) {
          throw new IllegalArgumentException("Invalid email settings in subscription group " + this.subsConfig.getId());
        }

        List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>(result.getValue());
        anomalyResultListOfGroup.sort(COMPARATOR_DESC);

        Properties emailClientConfigs = new Properties();
        emailClientConfigs.putAll(ConfigUtils.getMap(subsConfig.getAlertSchemes().get(PROP_EMAIL_SCHEME)));

        if (emailClientConfigs.get(PROP_RECIPIENTS) != null) {
          Map<String, Object> emailRecipients = ConfigUtils.getMap(emailClientConfigs.get(PROP_RECIPIENTS));
          if (emailRecipients.get(PROP_TO) == null || ConfigUtils.getList(emailRecipients.get(PROP_TO)).isEmpty()) {
            throw new IllegalArgumentException("No email recipients found in subscription group " + this.subsConfig.getId());
          }

          DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
              new HashSet<>(ConfigUtils.getList(emailRecipients.get(PROP_TO))),
              new HashSet<>(ConfigUtils.getList(emailRecipients.get(PROP_CC))),
              new HashSet<>(ConfigUtils.getList(emailRecipients.get(PROP_BCC))));
          sendEmail(prepareEmailContent(subsConfig, emailClientConfigs, anomalyResultListOfGroup, recipients));
          ThirdeyeMetricsUtil.emailAlertsSucesssCounter.inc();
        }
      } catch (Exception e) {
        ThirdeyeMetricsUtil.emailAlertsFailedCounter.inc();
        super.handleAlertFailure(result.getValue().size(), e);
      }
    }
  }

  @Override
  public void run() throws Exception {
    Preconditions.checkNotNull(result);
    if (result.getAllAnomalies().size() == 0) {
      LOG.info("Zero anomalies found, skipping email alert for {}", this.subsConfig.getId());
      return;
    }

    generateAndSendEmails(result);
  }
}
