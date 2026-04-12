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
package org.apache.pinot.controller.helix.core.ingest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.PublishCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Coordinates atomic segment publication for file-based INSERT INTO statements using the
 * segment replacement protocol ({@code startReplaceSegments} / {@code endReplaceSegments}).
 *
 * <p>This coordinator wraps the existing segment lineage mechanism in Pinot to ensure that
 * all segments produced by a single INSERT statement become visible atomically. If the
 * statement is aborted, {@code revertReplaceSegments} is called to roll back.
 *
 * <p>Each {@code publishId} returned by {@link #beginPublish} encodes the lineage entry ID.
 * The coordinator maintains a mapping from publishId to table name for use during commit
 * and abort.
 *
 * <p>This class is thread-safe for concurrent operations across different statements.
 */
public class FileInsertPublishCoordinator implements PublishCoordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileInsertPublishCoordinator.class);

  static final String STATEMENT_ID_KEY = "insert.statementId";

  private PinotHelixResourceManager _resourceManager;

  /**
   * Maps publishId (lineage entry ID) to table name with type for commit/abort.
   */
  private final ConcurrentHashMap<String, String> _publishIdToTable = new ConcurrentHashMap<>();

  /**
   * Maps publishId to the segment names for endReplaceSegments.
   */
  private final ConcurrentHashMap<String, List<String>> _publishIdToSegments = new ConcurrentHashMap<>();

  /**
   * Creates an instance. Call {@link #init(PinotConfiguration)} or
   * {@link #setResourceManager(PinotHelixResourceManager)} before use.
   */
  public FileInsertPublishCoordinator() {
  }

  /**
   * Creates an instance with explicit dependencies.
   *
   * @param resourceManager the Helix resource manager
   */
  public FileInsertPublishCoordinator(PinotHelixResourceManager resourceManager) {
    _resourceManager = resourceManager;
  }

  @Override
  public void init(PinotConfiguration config) {
    // Dependencies are injected via the constructor or setter.
  }

  /**
   * Sets the resource manager dependency.
   */
  public void setResourceManager(PinotHelixResourceManager resourceManager) {
    _resourceManager = resourceManager;
  }

  @Override
  public String beginPublish(String statementId, String tableNameWithType, List<String> segmentNames) {
    LOGGER.info("Beginning publish for statement {} on table {} with {} segments",
        statementId, tableNameWithType, segmentNames.size());

    Map<String, String> customMap = new HashMap<>();
    customMap.put(STATEMENT_ID_KEY, statementId);

    // Start the segment replacement protocol
    String lineageEntryId = _resourceManager.startReplaceSegments(
        tableNameWithType,
        Collections.emptyList(),  // segmentsFrom: empty for append
        segmentNames,             // segmentsTo: the new segments
        false,
        customMap
    );

    _publishIdToTable.put(lineageEntryId, tableNameWithType);
    _publishIdToSegments.put(lineageEntryId, segmentNames);

    LOGGER.info("Started segment replacement for statement {} with publish ID (lineage entry) {}",
        statementId, lineageEntryId);
    return lineageEntryId;
  }

  @Override
  public void commitPublish(String publishId) {
    String tableNameWithType = _publishIdToTable.get(publishId);
    if (tableNameWithType == null) {
      throw new IllegalStateException("Unknown publish ID: " + publishId);
    }

    List<String> segmentNames = _publishIdToSegments.get(publishId);
    LOGGER.info("Committing publish {} for table {} with segments {}", publishId, tableNameWithType, segmentNames);

    EndReplaceSegmentsRequest endRequest = new EndReplaceSegmentsRequest(segmentNames);
    _resourceManager.endReplaceSegments(tableNameWithType, publishId, endRequest);

    _publishIdToTable.remove(publishId);
    _publishIdToSegments.remove(publishId);

    LOGGER.info("Successfully committed publish {} for table {}", publishId, tableNameWithType);
  }

  @Override
  public void abortPublish(String publishId) {
    String tableNameWithType = _publishIdToTable.get(publishId);
    if (tableNameWithType == null) {
      LOGGER.warn("Unknown publish ID for abort: {}. May have already been cleaned up.", publishId);
      return;
    }

    LOGGER.info("Aborting publish {} for table {}", publishId, tableNameWithType);

    try {
      _resourceManager.revertReplaceSegments(tableNameWithType, publishId, false, null);
      LOGGER.info("Successfully reverted segment replacement for publish {}", publishId);
    } catch (Exception e) {
      LOGGER.error("Failed to revert segment replacement for publish {}", publishId, e);
    }

    _publishIdToTable.remove(publishId);
    _publishIdToSegments.remove(publishId);
  }

  /**
   * Returns the table name for a publish ID, visible for testing.
   */
  String getTableForPublishId(String publishId) {
    return _publishIdToTable.get(publishId);
  }
}
