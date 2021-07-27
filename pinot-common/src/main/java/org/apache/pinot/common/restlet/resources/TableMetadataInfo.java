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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;


/**
 * Class to represent aggregated segment metadata for a table.
 * - When return by server API, columnLengthMap/columnCardinalityMap is the total columnLength/columnCardinality across
 *   all segments on that server.
 * - When return by controller API, columnLengthMap/columnCardinalityMap is the average columnLength/columnCardinality
 *   across all segments for the given table.
 * - For diskSizeInBytes/numSegments/numRows, both server API and controller API will return the total value across all
 *   segments (if a segment has multiple replicas, only consider one replica).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetadataInfo {
  public String tableName = "";
  public long diskSizeInBytes = 0;
  public long numSegments = 0;
  public long numRows = 0;
  public Map<String, Double> columnLengthMap = new HashMap<>();
  public Map<String, Double> columnCardinalityMap = new HashMap<>();
}
