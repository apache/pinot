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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/** Index configuration for single text index containing multiple columns.
 *  It was added primarily to handle segments with text indexes on many columns that
 *  end up being impractical due to number of open files. */
public class MultiColumnTextIndexConfig extends BaseJsonConfig {

  // column names included in the text index
  private final List<String> _columns;

  // shared text index properties, similar to fieldConfigList.properties
  // applied to all columns
  private final Map<String, String> _properties;

  private final Map<String, Map<String, String>> _perColumnProperties;

  @JsonCreator
  public MultiColumnTextIndexConfig(
      @JsonProperty(value = "columns", required = true) List<String> columns,
      @JsonProperty(value = "properties") @Nullable Map<String, String> properties,
      @JsonProperty(value = "perColumnProperties") @Nullable Map<String, Map<String, String>> perColumnProperties) {
    _columns = columns;
    _properties = properties;
    _perColumnProperties = perColumnProperties;
  }

  public MultiColumnTextIndexConfig(@JsonProperty(value = "columns", required = true) List<String> columns) {
    this(columns, null, null);
  }

  public List<String> getColumns() {
    return _columns;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }

  @Nullable
  public Map<String, Map<String, String>> getPerColumnProperties() {
    return _perColumnProperties;
  }
}
