/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.EqualityUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TagOverrideConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TagOverrideConfig.class);

  @ConfigKey("realtimeConsuming")
  @ConfigDoc("Tag override for realtime consuming segments")
  private String realtimeConsuming;

  @ConfigKey("realtimeCompleted")
  @ConfigDoc("Tag override for realtime completed segments")
  private String realtimeCompleted;

  public String getRealtimeConsuming() {
    return realtimeConsuming;
  }

  public void setRealtimeConsuming(String realtimeConsuming) {
    this.realtimeConsuming = realtimeConsuming;
  }

  public String getRealtimeCompleted() {
    return realtimeCompleted;
  }

  public void setRealtimeCompleted(String realtimeCompleted) {
    this.realtimeCompleted = realtimeCompleted;
  }

  @Override
  public String toString() {
    return "TagOverrideConfig{" + "realtimeConsuming='" + realtimeConsuming + '\'' + ", realtimeCompleted="
        + realtimeCompleted + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    TagOverrideConfig that = (TagOverrideConfig) o;

    return EqualityUtils.isEqual(realtimeConsuming, that.realtimeConsuming) && EqualityUtils.isEqual(realtimeCompleted,
        that.realtimeCompleted);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(realtimeConsuming);
    result = EqualityUtils.hashCodeOf(result, realtimeCompleted);
    return result;
  }
}
