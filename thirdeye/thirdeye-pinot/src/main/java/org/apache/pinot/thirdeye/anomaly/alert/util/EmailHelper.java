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

package org.apache.pinot.thirdeye.anomaly.alert.util;

import org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration;
import org.apache.pinot.thirdeye.detection.alert.AlertUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stateless class to provide util methods to help build anomaly report
 */
public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);

  private EmailHelper() {
  }

  public static void sendEmailWithTextBody(HtmlEmail email, SmtpConfiguration smtpConfigutation, String subject,
      String textBody, String fromAddress, DetectionAlertFilterRecipients recipients) throws EmailException {
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email, subject, fromAddress, recipients);
  }

  /** Sends email according to the provided config. */
  private static void sendEmail(SmtpConfiguration config, HtmlEmail email, String subject,
      String fromAddress, DetectionAlertFilterRecipients recipients) throws EmailException {
    if (config != null) {
      email.setSubject(subject);
      LOG.info("Sending email to {}", recipients.toString());
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(fromAddress);
      email.setTo(AlertUtils.toAddress(recipients.getTo()));
      if (CollectionUtils.isNotEmpty(recipients.getCc())) {
        email.setCc(AlertUtils.toAddress(recipients.getCc()));
      }
      if (CollectionUtils.isNotEmpty(recipients.getBcc())) {
        email.setBcc(AlertUtils.toAddress(recipients.getBcc()));
      }
      email.send();
      LOG.info("Email sent with subject [{}] to address [{}]!", subject, recipients.toString());
    } else {
      LOG.error("No email config provided for email with subject [{}]!", subject);
    }
  }
}
