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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class DimensionTableConfig extends BaseJsonConfig {
  private final boolean _disablePreload;
  private final boolean _errorOnDuplicatePrimaryKey;
  private final boolean _enableUpsert;

  // Fields below are kept for backward compatibility with previous versions
  // of DimensionTableConfig.
  /**
   * Backward compatibility constructor that matches previous versions of DimensionTableConfig.
   * Delegates to the main constructor with upsert disabled by default.
   */
  public DimensionTableConfig(Boolean disablePreload, Boolean errorOnDuplicatePrimaryKey) {
    this(disablePreload, errorOnDuplicatePrimaryKey, false);
  }

  @JsonCreator
  public DimensionTableConfig(@JsonProperty(value = "disablePreload") Boolean disablePreload,
      @JsonProperty(value = "errorOnDuplicatePrimaryKey") Boolean errorOnDuplicatePrimaryKey,
      @JsonProperty(value = "enableUpsert") @JsonAlias("upsertEnabled") Boolean enableUpsert) {
    _disablePreload = disablePreload != null && disablePreload;
    _errorOnDuplicatePrimaryKey = errorOnDuplicatePrimaryKey != null && errorOnDuplicatePrimaryKey;
    _enableUpsert = enableUpsert != null && enableUpsert;
  }

  public boolean isDisablePreload() {
    return _disablePreload;
  }

  public boolean isErrorOnDuplicatePrimaryKey() {
    return _errorOnDuplicatePrimaryKey;
  }

  public boolean isUpsertEnabled() {
    return _enableUpsert;
  }
}
