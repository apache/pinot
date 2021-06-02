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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


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
public class TagOverrideConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Tag override for realtime consuming segments")
  private final String _realtimeConsuming;

  @JsonPropertyDescription("Tag override for realtime completed segments")
  private final String _realtimeCompleted;

  public TagOverrideConfig(@JsonProperty("realtimeConsuming") @Nullable String realtimeConsuming,
      @JsonProperty("realtimeCompleted") @Nullable String realtimeCompleted) {
    _realtimeConsuming = realtimeConsuming;
    _realtimeCompleted = realtimeCompleted;
  }

  @Nullable
  public String getRealtimeConsuming() {
    return _realtimeConsuming;
  }

  @Nullable
  public String getRealtimeCompleted() {
    return _realtimeCompleted;
  }
}
