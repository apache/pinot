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
package org.apache.pinot.query.runtime.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.routing.WorkerMetadata;


/**
 * {@code StageMetadata} is used to send plan fragment-level info about how to execute a stage physically.
 */
public class StageMetadata {
  private final List<WorkerMetadata> _workerMetadataList;
  private final Map<String, String> _customProperties;

  StageMetadata(List<WorkerMetadata> workerMetadataList, Map<String, String> customProperties) {
    _workerMetadataList = workerMetadataList;
    _customProperties = customProperties;
  }

  public List<WorkerMetadata> getWorkerMetadataList() {
    return _workerMetadataList;
  }

  public Map<String, String> getCustomProperties() {
    return Collections.unmodifiableMap(_customProperties);
  }

  public static class Builder {
    public static final String TABLE_NAME_KEY = "tableName";
    public static final String TIME_BOUNDARY_COLUMN_KEY = "timeBoundaryInfo.timeColumn";
    public static final String TIME_BOUNDARY_VALUE_KEY = "timeBoundaryInfo.timeValue";
    private List<WorkerMetadata> _workerMetadataList;
    private Map<String, String> _customProperties;

    public Builder() {
      _customProperties = new HashMap<>();
    }

    public Builder setWorkerMetadataList(List<WorkerMetadata> workerMetadataList) {
      _workerMetadataList = workerMetadataList;
      return this;
    }

    public Builder addTableName(String tableName) {
      _customProperties.put(TABLE_NAME_KEY, tableName);
      return this;
    }

    public Builder addTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
      _customProperties.put(TIME_BOUNDARY_COLUMN_KEY, timeBoundaryInfo.getTimeColumn());
      _customProperties.put(TIME_BOUNDARY_VALUE_KEY, timeBoundaryInfo.getTimeValue());
      return this;
    }

    public Builder addCustomProperties(Map<String, String> customPropertyMap) {
      _customProperties.putAll(customPropertyMap);
      return this;
    }

    public StageMetadata build() {
      return new StageMetadata(_workerMetadataList, _customProperties);
    }

    public void putAllCustomProperties(Map<String, String> customPropertyMap) {
      _customProperties.putAll(customPropertyMap);
    }
  }

  public static String getTableName(StageMetadata metadata) {
    return metadata.getCustomProperties().get(Builder.TABLE_NAME_KEY);
  }

  public static TimeBoundaryInfo getTimeBoundary(StageMetadata metadata) {
    String timeColumn = metadata.getCustomProperties().get(Builder.TIME_BOUNDARY_COLUMN_KEY);
    String timeValue = metadata.getCustomProperties().get(Builder.TIME_BOUNDARY_VALUE_KEY);
    return timeColumn != null && timeValue != null ? new TimeBoundaryInfo(timeColumn, timeValue) : null;
  }
}
