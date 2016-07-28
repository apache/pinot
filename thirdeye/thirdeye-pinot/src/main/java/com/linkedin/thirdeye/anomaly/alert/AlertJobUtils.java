package com.linkedin.thirdeye.anomaly.alert;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdeyeSmtpConfiguration;

public class AlertJobUtils {
  public static final String EMAIL_ADDRESS_SEPARATOR = ",";
  private static final Logger LOG = LoggerFactory.getLogger(AlertJobUtils.class);

  public static void sendEmailWithTextBody(ThirdeyeSmtpConfiguration smtpConfigutation, String subject,
      String textBody) throws EmailException {
    HtmlEmail email = new HtmlEmail();
    email.setSubject(subject);
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email);
  }

  public static void sendEmailWithHtml(ThirdeyeSmtpConfiguration smtpConfiguration, String subject,
      String htmlBody) throws EmailException {
    HtmlEmail email = new HtmlEmail();
    email.setSubject(subject);
    email.setHtmlMsg(htmlBody);
    sendEmail(smtpConfiguration, email);
  }

  /** Sends email according to the provided config. This method does not support html emails. */
  private static void sendEmail(ThirdeyeSmtpConfiguration config, HtmlEmail email) throws EmailException {
    if (config != null) {
      LOG.info("Sending email to {}", config.getToAddresses());
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(config.getFromAddress());
      for (String toAddress : config.getToAddresses().split(EMAIL_ADDRESS_SEPARATOR)) {
        email.addTo(toAddress);
      }
      email.send();
      LOG.info("Sent!");
    } else {
      LOG.error("No failure email configs provided!");
    }
  }
}
