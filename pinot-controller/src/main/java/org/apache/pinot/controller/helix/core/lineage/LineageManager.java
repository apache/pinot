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
package org.apache.pinot.controller.helix.core.lineage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Interface to update lineage metadata based on custom inputs and garbage collect lineage entries
 */
public interface LineageManager {

  /**
   * Update lineage based on customMap for start replace calls
   * @param tableConfig
   * @param lineageEntryId
   * @param customMap
   * @param lineage
   */
  void updateLineageForStartReplaceSegments(TableConfig tableConfig, String lineageEntryId,
      Map<String, String> customMap, SegmentLineage lineage);

  /**
   * Update lineage based on customMap for end replace calls
   * @param tableConfig
   * @param lineageEntryId
   * @param customMap
   * @param lineage
   */
  void updateLineageForEndReplaceSegments(TableConfig tableConfig, String lineageEntryId, Map<String, String> customMap,
      SegmentLineage lineage);

  /**
   * Update lineage based on customMap for revert replace calls
   * @param tableConfig
   * @param lineageEntryId
   * @param customMap
   * @param lineage
   */
  void updateLineageForRevertReplaceSegments(TableConfig tableConfig, String lineageEntryId,
      Map<String, String> customMap, SegmentLineage lineage);

  /**
   * Update lineage for retention purposes
   * @param tableConfig
   * @param lineage
   * @param allSegments
   * @param segmentsToDelete
   * @param consumingSegments
   */
  void updateLineageForRetention(TableConfig tableConfig, SegmentLineage lineage, List<String> allSegments,
      List<String> segmentsToDelete, Set<String> consumingSegments);
}
