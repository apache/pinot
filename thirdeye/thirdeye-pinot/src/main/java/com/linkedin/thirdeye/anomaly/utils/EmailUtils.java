package com.linkedin.thirdeye.anomaly.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EmailUtils {
  private static final Logger LOG = LoggerFactory.getLogger(EmailUtils.class);
  public static Pattern EMAIL_REGEX_PATTERN = Pattern.compile(
      "\\b[\\w.%-]+@[-.\\w]+\\.[A-Za-z]{2,4}\\b", Pattern.CASE_INSENSITIVE);
  /**
   * Check if given email is a valid email
   * @param email
   * @return
   */
  public static boolean isValidEmailAddress(String email) {
    return EMAIL_REGEX_PATTERN.matcher(email).matches();
  }

  /**
   * Return a list of valid email addresses
   * @param emails
   * @return
   */
  public static String getValidEmailAddresses(String emails) {
    List<String> emailAddressList= new ArrayList<>();
    Set<String> addedEmailAddresses = new HashSet<>();
    List<String> invalidEmailAddresses = new ArrayList<>();
    if (StringUtils.isBlank(emails)) {
      return null;
    } else {
      String[] emailArr = emails.split(",");
      for (String email : emailArr) {
        if (isValidEmailAddress(email) && !addedEmailAddresses.contains(email)) {
          emailAddressList.add(email);
          addedEmailAddresses.add(email);
        } else {
          invalidEmailAddresses.add(email);
        }
      }
    }
    if (invalidEmailAddresses.size() > 0) {
      LOG.warn("Found invalid email addresses, please verify the email addresses: {}", invalidEmailAddresses);
    }
    return StringUtils.join(emailAddressList, ",");
  }
}
