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
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;

import static org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter.*;


public class AlertFilterUtils {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";

  static final Set<String> PROP_TO_VALUE = new HashSet<>(Arrays.asList("test@example.com", "test@example.org"));
  static final Set<String> PROP_CC_VALUE = new HashSet<>(Arrays.asList("cctest@example.com", "cctest@example.org"));
  static final Set<String> PROP_BCC_VALUE = new HashSet<>(Arrays.asList("bcctest@example.com", "bcctest@example.org"));

  private static final Map<String, Object> ALERT_PROPS = new HashMap<>();

  static DetectionAlertFilterNotification makeEmailNotifications() {
    return makeEmailNotifications(new HashSet<String>());
  }

  static DetectionAlertFilterNotification makeEmailNotifications(Set<String> toRecipients) {
    Set<String> recipients = new HashSet<>(toRecipients);
    recipients.addAll(PROP_TO_VALUE);
    return makeEmailNotifications(recipients, PROP_CC_VALUE, PROP_BCC_VALUE);
  }

  static DetectionAlertFilterNotification makeEmailNotifications(Set<String> toRecipients, Set<String> ccRecipients, Set<String> bccRecipients) {
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, new HashSet<>(toRecipients));
    recipients.put(PROP_CC, new HashSet<>(ccRecipients));
    recipients.put(PROP_BCC, new HashSet<>(bccRecipients));

    Map<String, Object> emailRecipients = new HashMap<>();
    emailRecipients.put(PROP_RECIPIENTS, recipients);

    ALERT_PROPS.put(PROP_EMAIL_SCHEME, emailRecipients);

    return new DetectionAlertFilterNotification(ALERT_PROPS);
  }
}
