/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import com.linkedin.pinot.core.segment.index.loader.V3RemoveIndexException;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class V3DefaultColumnHandler extends BaseDefaultColumnHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(V3DefaultColumnHandler.class);

  private final SegmentDirectory.Writer _segmentWriter;

  public V3DefaultColumnHandler(File indexDir, Schema schema, SegmentMetadataImpl segmentMetadata,
      SegmentDirectory.Writer segmentWriter) {
    super(indexDir, schema, segmentMetadata);
    _segmentWriter = segmentWriter;
  }

  @Override
  protected void updateDefaultColumn(String column, DefaultColumnAction action)
      throws Exception {
    LOGGER.info("Starting default column action: {} on column: {}", action, column);

    // Column indices cannot be removed for segment format V3.
    // Throw exception to drop and re-download the segment.
    if (action.isRemoveAction()) {
      throw new V3RemoveIndexException(
          "Default value indices for column: " + column + " cannot be removed for V3 format segment.");
    }

    // Delete existing dictionary and forward index for the column. For V3, this is for error handling.
    removeColumnV1Indices(column);

    // Now we finished all the work needed for REMOVE action.
    // For ADD and UPDATE action, need to create new dictionary and forward index, and update column metadata.
    if (!action.isRemoveAction()) {
      createColumnV1Indices(column);

      // Write index to V3 format.
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      Preconditions.checkNotNull(fieldSpec);
      boolean isSingleValue = fieldSpec.isSingleValueField();
      File dictionaryFile = new File(_indexDir, column + V1Constants.Dict.FILE_EXTENSION);
      File forwardIndexFile;
      if (isSingleValue) {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      } else {
        forwardIndexFile = new File(_indexDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
      }
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, dictionaryFile, ColumnIndexType.DICTIONARY);
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, forwardIndexFile, ColumnIndexType.FORWARD_INDEX);
    }
  }
}
