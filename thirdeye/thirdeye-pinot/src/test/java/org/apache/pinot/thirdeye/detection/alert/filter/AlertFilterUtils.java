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

package org.apache.pinot.thirdeye.detection.alert.filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;

import static org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter.*;


public class AlertFilterUtils {

  public static final String PROP_RECIPIENTS = "recipients";
  public static final String PROP_TO = "to";
  public static final String PROP_CC = "cc";
  public static final String PROP_BCC = "bcc";

  public static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  public static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@example.com", "cctest@example.org"));
  public static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@example.com", "bcctest@example.org"));

  static DetectionAlertFilterNotification makeEmailNotifications(DetectionAlertConfigDTO config) {
    return makeEmailNotifications(config, new HashSet<String>());
  }

  static DetectionAlertFilterNotification makeEmailNotifications(DetectionAlertConfigDTO config, Set<String> toRecipients) {
    Set<String> recipients = new HashSet<>(toRecipients);
    recipients.addAll(PROP_TO_VALUE);
    return makeEmailNotifications(config, recipients, PROP_CC_VALUE, PROP_BCC_VALUE);
  }

  static DetectionAlertFilterNotification makeEmailNotifications(DetectionAlertConfigDTO config,
      Set<String> toRecipients, Set<String> ccRecipients, Set<String> bccRecipients) {
    Map<String, Object> alertProps = new HashMap<>();

    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, new HashSet<>(toRecipients));
    recipients.put(PROP_CC, new HashSet<>(ccRecipients));
    recipients.put(PROP_BCC, new HashSet<>(bccRecipients));

    Map<String, Object> emailRecipients = new HashMap<>();
    emailRecipients.put(PROP_RECIPIENTS, recipients);

    alertProps.put(PROP_EMAIL_SCHEME, emailRecipients);

    DetectionAlertConfigDTO subsConfig = SubscriptionUtils.makeChildSubscriptionConfig(config, alertProps, config.getReferenceLinks());

    return new DetectionAlertFilterNotification(subsConfig);
  }
}
