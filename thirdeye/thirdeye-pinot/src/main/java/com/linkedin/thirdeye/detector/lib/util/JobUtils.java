package com.linkedin.thirdeye.detector.lib.util;

import java.util.List;
import java.util.Properties;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.linkedin.thirdeye.detector.driver.FailureEmailConfiguration;

public class JobUtils {
  public static final String EMAIL_ADDRESS_SEPARATOR = ",";
  private static Splitter SEMICOLON_SPLITTER = Splitter.on(";").omitEmptyStrings();
  private static Splitter EQUALS_SPLITTER = Splitter.on("=").omitEmptyStrings();

  private static final Logger LOG = LoggerFactory.getLogger(JobUtils.class);

  /** Sends email according to the provided config. This method does not support html emails. */
  public static void sendFailureEmail(FailureEmailConfiguration config, String subject,
      String textBody) throws EmailException {
    if (config != null) {
      LOG.info("Sending failure email to {}", config.getToAddresses());
      HtmlEmail email = new HtmlEmail();
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
      email.setSubject("[ThirdEye Anomaly Detector] " + subject);
      // email.setHtmlMsg(htmlBody); Use this if you want html-enabled messages
      email.setTextMsg(textBody);
      email.send();
      LOG.info("Sent!");
    } else {
      LOG.error("No failure email configs provided!");
    }
  }

  public static Properties decodeCompactedProperties(String propStr) {
    Properties props = new Properties();
    for (String part : SEMICOLON_SPLITTER.split(propStr)) {
      List<String> kvPair = EQUALS_SPLITTER.splitToList(part);
      props.setProperty(kvPair.get(0), kvPair.get(1));
    }
    return props;
  }
}
