/**
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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * This config will allow specifying overrides to the tags derived from tenantConfig.
 * Supported override keys:
 *  - realtimeConsuming - value specifies the tag to be used for realtime consuming segments
 *  - realtimeCompleted - value specifies the tag to be used for realtime segments that are in ONLINE state.
 *
 * If a value is specified for the key 'realtimeCompleted' then realtime segments, once completed, will be moved
 * from the machines that consumed the rows in the segment to a pool of machines tagged with this value.
 *
 * These fields expect the complete tag name including the suffix, unlike the tenantConfig server and broker
 * where the suffix is added automatically. However the suffix in these tag names has to be one of OFFLINE
 * or REALTIME (for the present. We may extend it to have other tag suffixes later).
 *
 * Basic validation of the tags does happen when the table is being added. The validations include:
 * 1) checking if the suffix is correct (must be either _OFFLINE or _REALTIME)
 * 2) checking if instances with the tag exist
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TagOverrideConfig {

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

    return EqualityUtils.isEqual(realtimeConsuming, that.realtimeConsuming) && EqualityUtils
        .isEqual(realtimeCompleted, that.realtimeCompleted);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(realtimeConsuming);
    result = EqualityUtils.hashCodeOf(result, realtimeCompleted);
    return result;
  }
}
