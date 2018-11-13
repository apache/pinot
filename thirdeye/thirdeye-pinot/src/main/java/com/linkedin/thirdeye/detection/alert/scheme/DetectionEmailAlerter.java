/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.alert.scheme;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.alert.commons.EmailContentFormatterFactory;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterContext;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.alert.AlertUtils;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


public class DetectionEmailAlerter extends DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionEmailAlerter.class);

  private static final Comparator<AnomalyResult> COMPARATOR_DESC =
      (o1, o2) -> -1 * Long.compare(o1.getStartTime(), o2.getStartTime());
  private static final String DEFAULT_EMAIL_FORMATTER_TYPE = "MultipleAnomaliesEmailContentFormatter";
  private static final String EMAIL_WHITELIST_KEY = "emailWhitelist";

  private ThirdEyeAnomalyConfiguration teConfig;

  public DetectionEmailAlerter(DetectionAlertConfigDTO config, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) throws Exception {
    super(config, result);

    this.teConfig = thirdeyeConfig;
  }

  private Set<String> retainWhitelisted(Set<String> recipients, Collection<String> emailWhitelist) {
    if (recipients != null) {
      recipients.retainAll(emailWhitelist);
    }
    return recipients;
  }

  private void whitelistRecipients(DetectionAlertFilterRecipients recipients) {
    if (recipients != null) {
      List<String> emailWhitelist = ConfigUtils.getList(
          this.teConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY).get(EMAIL_WHITELIST_KEY));
      if (!emailWhitelist.isEmpty()) {
        recipients.setTo(retainWhitelisted(recipients.getTo(), emailWhitelist));
        recipients.setCc(retainWhitelisted(recipients.getCc(), emailWhitelist));
        recipients.setBcc(retainWhitelisted(recipients.getBcc(), emailWhitelist));
      }
    }
  }

  private void validateAlert(DetectionAlertFilterRecipients recipients, Set<MergedAnomalyResultDTO> anomalies) {
    Preconditions.checkNotNull(recipients);
    Preconditions.checkNotNull(anomalies);
    if (recipients.getTo() == null || recipients.getTo().isEmpty()) {
      throw new IllegalArgumentException("Email doesn't have any valid (whitelisted) recipients.");
    }
    if (anomalies.size() == 0) {
      throw new IllegalArgumentException("Zero anomalies found");
    }
  }

  /** Sends email according to the provided config. */
  private void sendEmail(EmailEntity entity) throws EmailException {
    HtmlEmail email = entity.getContent();
    SmtpConfiguration config = SmtpConfiguration.createFromProperties(this.teConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY));

    if (config == null) {
      LOG.error("No email configuration available. Skipping.");
      return;
    }

    email.setHostName(config.getSmtpHost());
    email.setSmtpPort(config.getSmtpPort());
    if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
      email.setAuthenticator(new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
      email.setSSLOnConnect(true);
    }
    email.send();

    int recipientCount = email.getToAddresses().size() + email.getCcAddresses().size() + email.getBccAddresses().size();
    LOG.info("Email sent with subject '{}' to {} recipients", email.getSubject(), recipientCount);
  }

  private void sendEmail(DetectionAlertFilterRecipients recipients, Set<MergedAnomalyResultDTO> anomalies) throws Exception {
    whitelistRecipients(recipients);
    validateAlert(recipients, anomalies);

    EmailContentFormatter emailContentFormatter =
        EmailContentFormatterFactory.fromClassName(DEFAULT_EMAIL_FORMATTER_TYPE);

    emailContentFormatter.init(new Properties(),
        EmailContentFormatterConfiguration.fromThirdEyeAnomalyConfiguration(this.teConfig));

    List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>();
    anomalyResultListOfGroup.addAll(anomalies);
    Collections.sort(anomalyResultListOfGroup, COMPARATOR_DESC);

    AlertConfigDTO alertConfig = new AlertConfigDTO();
    alertConfig.setName(this.config.getName());
    alertConfig.setFromAddress(this.config.getFrom());
    alertConfig.setSubjectType(this.config.getSubjectType());
    alertConfig.setReferenceLinks(this.config.getReferenceLinks());

    EmailEntity emailEntity = emailContentFormatter.getEmailEntity(alertConfig, null,
        "Thirdeye Alert : " + this.config.getName(), null, null, anomalyResultListOfGroup,
        new EmailContentFormatterContext());

    HtmlEmail email = emailEntity.getContent();
    email.setFrom(this.config.getFrom());
    email.setTo(AlertUtils.toAddress(recipients.getTo()));
    if (CollectionUtils.isNotEmpty(recipients.getCc())) {
      email.setCc(AlertUtils.toAddress(recipients.getCc()));
    }
    if (CollectionUtils.isNotEmpty(recipients.getBcc())) {
      email.setBcc(AlertUtils.toAddress(recipients.getBcc()));
    }

    sendEmail(emailEntity);
  }

  private void generateAndSendEmails(DetectionAlertFilterResult detectionResult) throws Exception {
    LOG.info("Sending Email alert for {}", config.getId());
    Preconditions.checkNotNull(detectionResult.getResult());
    for (Map.Entry<DetectionAlertFilterRecipients, Set<MergedAnomalyResultDTO>> entry : detectionResult.getResult().entrySet()) {
      DetectionAlertFilterRecipients recipients = entry.getKey();
      Set<MergedAnomalyResultDTO> anomalies = entry.getValue();

      try {
        sendEmail(recipients, anomalies);
      } catch (IllegalArgumentException e) {
        LOG.warn("Skipping! Found illegal arguments while sending {} anomalies to recipient {} for alert {}."
            + " Exception message: ", anomalies.size(), recipients, config.getId(), e);
      }
    }
  }

  @Override
  public void run() throws Exception {
    Preconditions.checkNotNull(result);
    if (result.getAllAnomalies().size() == 0) {
      LOG.info("Zero anomalies found, skipping sending iris alert for {}", config.getId());
      return;
    }

    generateAndSendEmails(result);
  }
}
