package com.linkedin.thirdeye.anomaly.alert;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.SmtpConfiguration;

public class AlertJobUtils {
  public static final String EMAIL_ADDRESS_SEPARATOR = ",";
  private static final Logger LOG = LoggerFactory.getLogger(AlertJobUtils.class);

  public static void sendEmailWithTextBody(HtmlEmail email, SmtpConfiguration smtpConfigutation, String subject,
      String textBody, String fromAddress, String toAddress) throws EmailException {
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email, subject, fromAddress, toAddress);
  }

  public static void sendEmailWithHtml(HtmlEmail email, SmtpConfiguration smtpConfiguration, String subject,
      String htmlBody, String fromAddress, String toAddress) throws EmailException {
    email.setHtmlMsg(htmlBody);
    sendEmail(smtpConfiguration, email, subject, fromAddress, toAddress);
  }

  /** Sends email according to the provided config. */
  private static void sendEmail(SmtpConfiguration config, HtmlEmail email, String subject, String fromAddress, String toAddress) throws EmailException {
    if (config != null) {
      email.setSubject(subject);
      LOG.info("Sending email to {}", toAddress);
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(fromAddress);
      for (String address : toAddress.split(EMAIL_ADDRESS_SEPARATOR)) {
        email.addTo(address);
      }
      email.send();
      LOG.info("Sent!");
    } else {
      LOG.error("No failure email configs provided!");
    }
  }
}
