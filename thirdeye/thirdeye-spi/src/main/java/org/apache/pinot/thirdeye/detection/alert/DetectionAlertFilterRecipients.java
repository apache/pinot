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

package org.apache.pinot.thirdeye.detection.alert;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


/**
 * Container class for email alert recipients
 */
public class DetectionAlertFilterRecipients {
  Set<String> to;
  Set<String> cc;
  Set<String> bcc;

  public DetectionAlertFilterRecipients(Collection<String> to, Collection<String> cc, Collection<String> bcc) {
    this.to = new HashSet<>(to);
    this.cc = new HashSet<>(cc);
    this.bcc = new HashSet<>(bcc);
  }

  public DetectionAlertFilterRecipients(Set<String> to) {
    this.to = to;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("TO", Arrays.toString((getTo() != null) ? getTo().toArray() : new String[]{}))
        .add("CC", Arrays.toString((getCc() != null) ? getCc().toArray() : new String[]{}))
        .add("BCC", Arrays.toString((getBcc() != null) ? getBcc().toArray() : new String[]{}))
        .toString();
  }
}
