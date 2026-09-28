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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class V1DefaultColumnHandler extends BaseDefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(V1DefaultColumnHandler.class);

  public V1DefaultColumnHandler(File indexDir, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig, SegmentDirectory.Writer segmentWriter) {
    super(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
  }

  @Override
  protected boolean updateDefaultColumn(String column, DefaultColumnAction action)
      throws Exception {
    LOGGER.info("Starting default column action: {} on column: {}", action, column);

    if (action.isUpdateAction()) {
      try (PreparedColumnUpdate preparedColumnUpdate = prepareColumnUpdate(column)) {
        if (preparedColumnUpdate == null) {
          return false;
        }
        removeColumnIndices(column);
        File[] indexFiles = preparedColumnUpdate.getIndexDir().listFiles();
        if (indexFiles == null) {
          throw new IllegalStateException(
              "Failed to list prepared indices for default column: " + column);
        }
        for (File indexFile : indexFiles) {
          FileUtils.moveFileToDirectory(indexFile, _indexDir, false);
        }
        commitPreparedColumnMetadata(column, preparedColumnUpdate);
        return true;
      }
    }

    // For REMOVE action, delete existing dictionary and forward index, and remove column metadata
    if (action.isRemoveAction()) {
      removeColumnIndices(column);
      return true;
    }

    // For ADD action, create new dictionary and forward index, and update column metadata
    if (action.isAddAction()) {
      return createColumnV1Indices(column);
    }
    throw new IllegalStateException("Unsupported default column action: " + action);
  }
}
