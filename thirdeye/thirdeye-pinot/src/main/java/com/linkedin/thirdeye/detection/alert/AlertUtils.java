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

package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.mysql.jdbc.StringUtils;
import java.util.Collection;
import java.util.Collections;
import javax.mail.internet.InternetAddress;
import org.apache.commons.collections.CollectionUtils;


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
        && !anomaly.getFeedback().getFeedbackType().isUnresolved();
  }

  /**
   * Helper to convert a collection of email strings into {@code InternetAddress} instances, filtering
   * out invalid addresses and nulls.
   *
   * @param emailCollection collection of email address strings
   * @return filtered collection of InternetAddress objects
   */
  public static Collection<InternetAddress> toAddress(Collection<String> emailCollection) {
    if (CollectionUtils.isEmpty(emailCollection)) {
      return Collections.emptySet();
    }
    return Collections2.filter(Collections2.transform(emailCollection,
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
