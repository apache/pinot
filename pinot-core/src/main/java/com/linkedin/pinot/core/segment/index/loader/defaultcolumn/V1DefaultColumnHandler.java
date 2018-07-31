/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.loader.defaultcolumn;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class V1DefaultColumnHandler extends BaseDefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(V1DefaultColumnHandler.class);

  public V1DefaultColumnHandler(File indexDir, Schema schema, SegmentMetadataImpl segmentMetadata) {
    super(indexDir, schema, segmentMetadata);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateDefaultColumn(String column, DefaultColumnAction action) throws Exception {
    LOGGER.info("Starting default column action: {} on column: {}", action, column);

    // For UPDATE and REMOVE action, delete existing dictionary and forward index, and remove column metadata
    if (action.isUpdateAction() || action.isRemoveAction()) {
      removeColumnV1Indices(column);
    }

    // For ADD and UPDATE action, create new dictionary and forward index, and update column metadata
    if (action.isAddAction() || action.isUpdateAction()) {
      createColumnV1Indices(column);
    }
  }
}
