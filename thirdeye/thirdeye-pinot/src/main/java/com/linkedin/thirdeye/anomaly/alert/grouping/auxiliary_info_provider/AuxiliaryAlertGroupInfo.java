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

package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import java.util.Objects;

public class AuxiliaryAlertGroupInfo {
  private boolean skipGroupAlert = false;
  private String auxiliaryRecipients = "";
  private String groupTag = "";

  /**
   * True means this group's alert should be skipped.
   * @return if this group's alert should be skipped.
   */
  public boolean isSkipGroupAlert() {
    return skipGroupAlert;
  }

  /**
   * Set if this group's alert should be skipped.
   * @param skipGroupAlert the flag that indicates if this group's alert should be skipped.
   */
  public void setSkipGroupAlert(boolean skipGroupAlert) {
    this.skipGroupAlert = skipGroupAlert;
  }

  /**
   * Returns auxiliary recipients to be added to the group alert.
   * @return auxiliary recipients to be added to the group alert.
   */
  public String getAuxiliaryRecipients() {
    return auxiliaryRecipients;
  }

  /**
   * Sets auxiliary recipients to be added to the group alert. The recipients should be complete email address and
   * are separated by commas.
   * @param auxiliaryRecipients the auxiliary recipients to be added to the group alert.
   */
  public void setAuxiliaryRecipients(String auxiliaryRecipients) {
    this.auxiliaryRecipients = auxiliaryRecipients;
  }

  /**
   * Returns the string to be added to the alert's subject.
   * @return the string to be added to the alert's subject.
   */
  public String getGroupTag() {
    return groupTag;
  }

  /**
   * Sets the string to be added to the alert's subject.
   * @param groupTag the string to be added to the alert's subject.
   */
  public void setGroupTag(String groupTag) {
    this.groupTag = groupTag;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuxiliaryAlertGroupInfo that = (AuxiliaryAlertGroupInfo) o;
    return isSkipGroupAlert() == that.isSkipGroupAlert() && Objects
        .equals(getAuxiliaryRecipients(), that.getAuxiliaryRecipients()) && Objects
        .equals(getGroupTag(), that.getGroupTag());
  }

  @Override
  public int hashCode() {
    return Objects.hash(isSkipGroupAlert(), getAuxiliaryRecipients(), getGroupTag());
  }
}
