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
package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;


/// Additional metadata for the [org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan].
public class TableScanMetadata {
  private final Set<String> _scannedTables;
  /// Stores workerId, which is an integer, to the segment mapping. The segment mapping is a map from table-type to list
  /// of segments. Used for physical tables.
  ///
  /// ```
  /// {
  ///   0: {
  ///     "OFFLINE": ["segment1", "segment2"],
  ///     "REALTIME": ["segment3"]
  ///   },
  ///   1: {
  ///     ...
  ///   }
  /// }
  /// ```
  private final Map<Integer, Map<String, List<String>>> _workedIdToSegmentsMap;
  /// Stores workerId to the segment mapping for logical tables. The segment mapping is a map from
  /// physical table name (with type suffix) to list of segments. Used for logical tables.
  ///
  /// ```
  /// {
  ///   0: {
  ///     "orders_OFFLINE": ["segment1", "segment2"],
  ///     "orders_REALTIME": ["segment3"],
  ///     "orders_archive_OFFLINE": ["segment4"]
  ///   },
  ///   1: {
  ///     ...
  ///   }
  /// }
  /// ```
  @Nullable
  private final Map<Integer, Map<String, List<String>>> _workerIdToLogicalTableSegmentsMap;
  /// Stores workerId to the optional segments of the worker, keyed like the segment map in use (table type, or
  /// physical table name for logical tables). Only workers with optional segments have an entry; `null` when none has.
  @Nullable
  private final Map<Integer, Map<String, List<String>>> _workerIdToOptionalSegmentsMap;
  private final Map<String, String> _tableOptions;
  private final Map<String, Set<String>> _unavailableSegmentsMap;
  @Nullable
  private final TimeBoundaryInfo _timeBoundaryInfo;
  /// Indicates whether this is a logical table scan.
  private final boolean _isLogicalTable;

  /// Constructor for physical table scan metadata.
  public TableScanMetadata(Set<String> scannedTables, Map<Integer, Map<String, List<String>>> workedIdToSegmentsMap,
      @Nullable Map<Integer, Map<String, List<String>>> workerIdToOptionalSegmentsMap,
      Map<String, String> tableOptions, Map<String, Set<String>> unavailableSegmentsMap,
      @Nullable TimeBoundaryInfo timeBoundaryInfo) {
    _scannedTables = scannedTables;
    _workedIdToSegmentsMap = workedIdToSegmentsMap;
    _workerIdToLogicalTableSegmentsMap = null;
    _workerIdToOptionalSegmentsMap = workerIdToOptionalSegmentsMap;
    _tableOptions = tableOptions;
    _unavailableSegmentsMap = unavailableSegmentsMap;
    _timeBoundaryInfo = timeBoundaryInfo;
    _isLogicalTable = false;
  }

  /// Constructor for logical table scan metadata.
  public TableScanMetadata(Set<String> scannedTables,
      Map<Integer, Map<String, List<String>>> workerIdToLogicalTableSegmentsMap,
      @Nullable Map<Integer, Map<String, List<String>>> workerIdToOptionalSegmentsMap,
      Map<String, String> tableOptions, Map<String, Set<String>> unavailableSegmentsMap,
      @Nullable TimeBoundaryInfo timeBoundaryInfo, boolean isLogicalTable) {
    _scannedTables = scannedTables;
    _workerIdToOptionalSegmentsMap = workerIdToOptionalSegmentsMap;
    if (isLogicalTable) {
      _workedIdToSegmentsMap = null;
      _workerIdToLogicalTableSegmentsMap = workerIdToLogicalTableSegmentsMap;
    } else {
      _workedIdToSegmentsMap = workerIdToLogicalTableSegmentsMap;
      _workerIdToLogicalTableSegmentsMap = null;
    }
    _tableOptions = tableOptions;
    _unavailableSegmentsMap = unavailableSegmentsMap;
    _timeBoundaryInfo = timeBoundaryInfo;
    _isLogicalTable = isLogicalTable;
  }

  public Set<String> getScannedTables() {
    return _scannedTables;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkedIdToSegmentsMap() {
    return _workedIdToSegmentsMap;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkerIdToLogicalTableSegmentsMap() {
    return _workerIdToLogicalTableSegmentsMap;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkerIdToOptionalSegmentsMap() {
    return _workerIdToOptionalSegmentsMap;
  }

  public Map<String, String> getTableOptions() {
    return _tableOptions;
  }

  public Map<String, Set<String>> getUnavailableSegmentsMap() {
    return _unavailableSegmentsMap;
  }

  @Nullable
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public boolean isLogicalTable() {
    return _isLogicalTable;
  }
}
