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
package org.apache.pinot.core.segment.index.loader.defaultcolumn;

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.loader.V3RemoveIndexException;
import org.apache.pinot.core.segment.index.loader.V3UpdateIndexException;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class V3DefaultColumnHandler extends BaseDefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(V3DefaultColumnHandler.class);

  public V3DefaultColumnHandler(File indexDir, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig, Schema schema, SegmentDirectory.Writer segmentWriter) {
    super(indexDir, segmentMetadata, indexLoadingConfig, schema, segmentWriter);
  }

  @Override
  protected boolean updateDefaultColumn(String column, DefaultColumnAction action)
      throws Exception {
    LOGGER.info("Starting default column action: {} on column: {}", action, column);

    // For V3 segment format, only support ADD action
    // For UPDATE and REMOVE action, throw exception to drop and re-download the segment
    if (action.isUpdateAction()) {
      throw new V3UpdateIndexException(
          "Default value indices for column: " + column + " cannot be updated for V3 format segment.");
    }

    if (action.isRemoveAction()) {
      throw new V3RemoveIndexException(
          "Default value indices for column: " + column + " cannot be removed for V3 format segment.");
    }

    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    Preconditions.checkNotNull(fieldSpec);
    boolean isSingleValue = fieldSpec.isSingleValueField();
    // Create new dictionary and forward index, and update column metadata
    if (createColumnV1Indices(column)) {
      // Write index to V3 format.
      File dictionaryFile = new File(_indexDir, column + V1Constants.Dict.FILE_EXTENSION);
      File forwardIndexFile;
      if (isSingleValue) {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (!forwardIndexFile.exists()) {
          forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        }
      } else {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
      }
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, dictionaryFile, ColumnIndexType.DICTIONARY);
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, forwardIndexFile, ColumnIndexType.FORWARD_INDEX);
      return true;
    } else {
      return false;
    }
  }
}
