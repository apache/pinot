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
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class SegmentPartitionConfig extends BaseJsonConfig {
  @Deprecated
  public static final int INVALID_NUM_PARTITIONS = -1;

  private final Map<String, ColumnPartitionConfig> _columnPartitionMap;

  @JsonCreator
  public SegmentPartitionConfig(@JsonProperty(value = "columnPartitionMap", required = true)
      Map<String, ColumnPartitionConfig> columnPartitionMap) {
    Preconditions.checkArgument(columnPartitionMap != null, "'columnPartitionMap' must be configured");
    _columnPartitionMap = columnPartitionMap;
  }

  public Map<String, ColumnPartitionConfig> getColumnPartitionMap() {
    return _columnPartitionMap;
  }

  @Nullable
  public ColumnPartitionConfig getColumnPartitionConfig(String column) {
    return _columnPartitionMap.get(column);
  }

  @Deprecated
  @Nullable
  public String getFunctionName(String column) {
    ColumnPartitionConfig columnPartitionConfig = _columnPartitionMap.get(column);
    return (columnPartitionConfig != null) ? columnPartitionConfig.getFunctionName() : null;
  }

  @Deprecated
  public int getNumPartitions(String column) {
    ColumnPartitionConfig config = _columnPartitionMap.get(column);
    return (config != null) ? config.getNumPartitions() : INVALID_NUM_PARTITIONS;
  }

  @Deprecated
  @Nullable
  public Map<String, String> getFunctionConfig(String column) {
    ColumnPartitionConfig config = _columnPartitionMap.get(column);
    return (config != null) ? config.getFunctionConfig() : null;
  }
}
