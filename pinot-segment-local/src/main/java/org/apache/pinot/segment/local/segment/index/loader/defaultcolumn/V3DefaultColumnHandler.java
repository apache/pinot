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

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
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

    // For UPDATE and REMOVE action, delete existing dictionary and forward index, and remove column metadata
    if (action.isUpdateAction() || action.isRemoveAction()) {
      removeColumnIndices(column);
      if (action.isRemoveAction()) {
        // No more to do for REMOVE action.
        return true;
      }
    }
    // For ADD and UPDATE action, need to create new dictionary and forward index,
    // update column metadata, and write out with V3 format.
    if (!createColumnV1Indices(column)) {
      return false;
    }
    // Write index to V3 format
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    Preconditions.checkNotNull(fieldSpec);
    boolean isSingleValue = fieldSpec.isSingleValueField();
    boolean forwardIndexDisabled = !isSingleValue && isForwardIndexDisabled(column);
    File forwardIndexFile = null;
    File invertedIndexFile = null;
    if (isSingleValue) {
      forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      if (!forwardIndexFile.exists()) {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      }
    } else {
      if (forwardIndexDisabled) {
        // An inverted index is created instead of forward index for multi-value columns with forward index disabled
        invertedIndexFile = new File(_indexDir, column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
      } else {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
      }
    }
    if (forwardIndexFile != null) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, forwardIndexFile, StandardIndexes.forward());
    }
    if (invertedIndexFile != null) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, invertedIndexFile, StandardIndexes.inverted());
    }
    File dictionaryFile = new File(_indexDir, column + V1Constants.Dict.FILE_EXTENSION);
    LoaderUtils.writeIndexToV3Format(_segmentWriter, column, dictionaryFile, StandardIndexes.dictionary());

    File nullValueVectorFile = new File(_indexDir, column + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);
    if (nullValueVectorFile.exists()) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, nullValueVectorFile, StandardIndexes.nullValueVector());
    }
    return true;
  }
}
