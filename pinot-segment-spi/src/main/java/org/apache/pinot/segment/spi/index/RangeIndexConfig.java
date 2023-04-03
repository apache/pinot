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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;


public class RangeIndexConfig extends IndexConfig {
  public static final RangeIndexConfig DEFAULT = new RangeIndexConfig(true, 2);
  public static final RangeIndexConfig DISABLED = new RangeIndexConfig(false, null);

  private final int _version;

  public RangeIndexConfig(int version) {
    this(true, version);
  }

  @JsonCreator
  public RangeIndexConfig(@JsonProperty("enabled") Boolean enabled,
      @JsonProperty("version") @Nullable Integer version) {
    super(enabled != null && enabled && version != null);
    _version = version != null ? version : 2;
  }

  public int getVersion() {
    return _version;
  }
}
