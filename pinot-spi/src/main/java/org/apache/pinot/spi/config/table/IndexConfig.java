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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * This is the base class used to configure indexes.
 *
 * The common logic between all indexes is that they can be enabled or disabled.
 *
 * Indexes that do not require extra configuration can directly use this class.
 */
public class IndexConfig extends BaseJsonConfig {
  public static final IndexConfig ENABLED = new IndexConfig(true);
  public static final IndexConfig DISABLED = new IndexConfig(false);
  private final boolean _enabled;

  /**
   * This is used as the default constructor for subclasses. The attribute {@link #_enabled} will be initialized to
   * {@code enabled == null || enabled}, so it will be false if and only if {@code Boolean.FALSE.equals(enabled)}.
   */
  @JsonCreator
  public IndexConfig(@JsonProperty("enabled") Boolean enabled) {
    _enabled = enabled == null || enabled;
  }

  public boolean isEnabled() {
    return _enabled;
  }
}
