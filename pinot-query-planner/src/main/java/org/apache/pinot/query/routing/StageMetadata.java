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
package org.apache.pinot.query.routing;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;


/**
 * {@code StageMetadata} is used to send plan fragment-level info about how to execute a stage physically.
 */
public class StageMetadata {
  private final int _stageId;
  private final List<WorkerMetadata> _workerMetadataList;
  private final Map<String, String> _customProperties;

  public StageMetadata(int stageId, List<WorkerMetadata> workerMetadataList, Map<String, String> customProperties) {
    _stageId = stageId;
    _workerMetadataList = workerMetadataList;
    _customProperties = customProperties;
  }

  public int getStageId() {
    return _stageId;
  }

  public List<WorkerMetadata> getWorkerMetadataList() {
    return _workerMetadataList;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public String getTableName() {
    return _customProperties.get(DispatchablePlanFragment.TABLE_NAME_KEY);
  }

  public TimeBoundaryInfo getTimeBoundary() {
    String timeColumn = _customProperties.get(DispatchablePlanFragment.TIME_BOUNDARY_COLUMN_KEY);
    String timeValue = _customProperties.get(DispatchablePlanFragment.TIME_BOUNDARY_VALUE_KEY);
    return timeColumn != null && timeValue != null ? new TimeBoundaryInfo(timeColumn, timeValue) : null;
  }
}
