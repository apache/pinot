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


public class DimensionTableConfig extends BaseJsonConfig {
  private final boolean _disablePreload;
  private final boolean _disallowDuplicatePrimaryKey;

  @JsonCreator
  public DimensionTableConfig(@JsonProperty(value = "disablePreload") Boolean disablePreload,
      @JsonProperty(value = "disallowDuplicatePrimaryKey") Boolean disallowDuplicatePrimaryKey) {
    _disablePreload = disablePreload != null && disablePreload;
    _disallowDuplicatePrimaryKey = disallowDuplicatePrimaryKey != null && disallowDuplicatePrimaryKey;
  }

  public boolean isDisablePreload() {
    return _disablePreload;
  }

  public boolean disallowDuplicatePrimaryKey() {
    return _disallowDuplicatePrimaryKey;
  }
}
