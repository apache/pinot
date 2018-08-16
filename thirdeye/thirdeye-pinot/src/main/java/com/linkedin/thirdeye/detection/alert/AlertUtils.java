package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collection;
import javax.mail.internet.InternetAddress;


public class AlertUtils {
  private AlertUtils() {
    //left blank
  }

  /**
   * Helper to determine presence of user-feedback for an anomaly
   *
   * @param anomaly anomaly
   * @return {@code true} if feedback exists and is TRUE or FALSE, {@code false} otherwise
   */
  public static boolean hasFeedback(MergedAnomalyResultDTO anomaly) {
    return anomaly.getFeedback() != null
        && !AnomalyFeedbackType.NO_FEEDBACK.equals(anomaly.getFeedback().getFeedbackType());
  }

  /**
   * Helper to convert a collection of email strings into {@code InternetAddress} instances, filtering
   * out invalid addresses and nulls.
   *
   * @param strings email address strings
   * @return filtered collection of InternetAddress objects
   */
  public static Collection<InternetAddress> toAddress(Collection<String> strings) {
    return Collections2.filter(Collections2.transform(strings,
        new Function<String, InternetAddress>() {
          @Override
          public InternetAddress apply(String s) {
            try {
              return InternetAddress.parse(s)[0];
            } catch (Exception e) {
              return null;
            }
          }
        }),
        new Predicate<InternetAddress>() {
          @Override
          public boolean apply(InternetAddress internetAddress) {
            return internetAddress != null;
          }
        });
  }

}
