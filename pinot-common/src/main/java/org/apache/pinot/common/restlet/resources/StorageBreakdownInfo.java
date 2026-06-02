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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


/// Storage breakdown by tier, reported under the `storageBreakdown` key in the
/// `GET /tables/{tableName}/size` and `GET /tables/{tableName}/metadata` responses.
/// Maps tier name (e.g. `"default"`, `"hotTier"`) to segment count and per-replica size.
@JsonIgnoreProperties(ignoreUnknown = true)
public class StorageBreakdownInfo {

  private final Map<String, TierInfo> _tiers;

  @JsonCreator
  public StorageBreakdownInfo(@JsonProperty("tiers") Map<String, TierInfo> tiers) {
    _tiers = tiers;
  }

  public Map<String, TierInfo> getTiers() {
    return _tiers;
  }

  /// Segment count and per-replica on-disk size for a single storage tier.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TierInfo {
    private final int _count;
    private final long _sizePerReplicaInBytes;

    @JsonCreator
    public TierInfo(@JsonProperty("count") int count,
        @JsonProperty("sizePerReplicaInBytes") long sizePerReplicaInBytes) {
      _count = count;
      _sizePerReplicaInBytes = sizePerReplicaInBytes;
    }

    public int getCount() {
      return _count;
    }

    public long getSizePerReplicaInBytes() {
      return _sizePerReplicaInBytes;
    }
  }
}
