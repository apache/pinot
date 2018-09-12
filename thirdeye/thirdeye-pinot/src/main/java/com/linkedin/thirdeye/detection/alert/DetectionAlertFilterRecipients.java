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

import java.util.Objects;
import java.util.Set;


/**
 * Container class for email alert recipients
 */
public class DetectionAlertFilterRecipients {
  Set<String> to;
  Set<String> cc;
  Set<String> bcc;

  public DetectionAlertFilterRecipients(Set<String> to, Set<String> cc, Set<String> bcc) {
    this.to = to;
    this.cc = cc;
    this.bcc = bcc;
  }

  public DetectionAlertFilterRecipients() {
  }

  public Set<String> getTo() {
    return to;
  }

  public DetectionAlertFilterRecipients setTo(Set<String> to) {
    this.to = to;
    return this;
  }

  public Set<String> getCc() {
    return cc;
  }

  public DetectionAlertFilterRecipients setCc(Set<String> cc) {
    this.cc = cc;
    return this;
  }

  public Set<String> getBcc() {
    return bcc;
  }

  public DetectionAlertFilterRecipients setBcc(Set<String> bcc) {
    this.bcc = bcc;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionAlertFilterRecipients that = (DetectionAlertFilterRecipients) o;
    return Objects.equals(to, that.to) && Objects.equals(cc, that.cc) && Objects.equals(bcc, that.bcc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(to, cc, bcc);
  }
}
