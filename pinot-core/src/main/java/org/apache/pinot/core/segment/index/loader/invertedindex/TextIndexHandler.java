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
package org.apache.pinot.core.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import org.apache.pinot.core.segment.creator.TextIndexType;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.TEXT_INDEX_TYPE;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.getKeyFor;


public class TextIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _textIndexColumns = new HashSet<>();

  public TextIndexHandler(@Nonnull File indexDir, @Nonnull SegmentMetadataImpl segmentMetadata,
      @Nonnull Set<String> textIndexColumns, @Nonnull SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    for (String column : textIndexColumns) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        _textIndexColumns.add(columnMetadata);
      }
    }
  }

  /**
   * Create text index for column during segment load time. Currently text index is always
   * created (if enabled on a column) during segment generation (true for both offline
   * and realtime segments). So this function is a NO-OP for case when a new segment is loaded
   * after creation. However, when segment reload is issued in the following scenarios, we generate
   * text index.
   *
   * SCENARIO 1: user enables text index on an existing column (table config change)
   * SCENARIO 2: user adds a new column and enables text index (both schema and table config change)
   *
   * This function is a NO-OP for the above two cases. Later we can also add a segment generator
   * config option to not necessarily generate text index during segment generation. When we do
   * so, this function should be able to take care of that scenario too.
   *
   * For scenario 2, {@link org.apache.pinot.core.segment.index.loader.defaultcolumn.V3DefaultColumnHandler}
   * would have already added the forward index for the column with default value. We use the forward
   * index here to get the raw data and build text index.
   *
   * @throws IOException
   */
  public void createTextIndexesOnSegmentLoad()
      throws Exception {
    for (ColumnMetadata columnMetadata : _textIndexColumns) {
      checkUnsupportedOperationsForTextIndex(columnMetadata);
      createTextIndexForColumn(columnMetadata);
    }
  }

  /**
   * Right now the text index is supported on RAW (non-dictionary encoded)
   * single-value STRING columns. Eventually we will relax the constraints
   * step by step.
   * For example, later on user should be able to create text index on
   * a dictionary encoded STRING column that also has native Pinot's inverted
   * index. We can also support it on BYTE columns later.
   * @param columnMetadata metadata for column
   */
  private void checkUnsupportedOperationsForTextIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.hasDictionary()) {
      throw new UnsupportedOperationException("Text index is currently not supported on dictionary encoded column: "+column);
    }

    if (columnMetadata.isSorted()) {
      // since Pinot's current implementation doesn't support raw sorted columns,
      // we need to check for this too
      throw new UnsupportedOperationException("Text index is currently not supported on sorted columns: "+column);
    }

    if (!columnMetadata.isSingleValue()) {
      throw new UnsupportedOperationException("Text index is currently not supported on multi-value columns: "+column);
    }

    if (columnMetadata.getDataType() != FieldSpec.DataType.STRING) {
      throw new UnsupportedOperationException("Text index is currently only supported on STRING columns: "+column);
    }
  }

  private void createTextIndexForColumn(ColumnMetadata columnMetadata)
      throws Exception {
    String column = columnMetadata.getColumnName();
    if (_segmentWriter.hasIndexFor(column, ColumnIndexType.TEXT_INDEX)) {
      // Skip creating text index if already exists.
      LOGGER.info("Found text index for column: {}, in segment: {}", column, _segmentName);
      return;
    }
    int numDocs = columnMetadata.getTotalDocs();
    LOGGER.info("Creating new text index for column: {} in segment: {}", column, _segmentName);
    File segmentIndexDir = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, _segmentVersion);
    try (LuceneTextIndexCreator textIndexCreator = new LuceneTextIndexCreator(column, segmentIndexDir, true)) {
      try (DataFileReader forwardIndexReader = getForwardIndexReader(columnMetadata)) {
        VarByteChunkSingleValueReader forwardIndex = (VarByteChunkSingleValueReader) forwardIndexReader;
        for (int docID = 0; docID < numDocs; docID++) {
          Object docToAdd = forwardIndex.getString(docID);
          textIndexCreator.addDoc(docToAdd, docID);
        }
        textIndexCreator.seal();
      }
    }
    LOGGER.info("Created text index for column: {} in segment: {}", column, _segmentName);
    PropertiesConfiguration properties = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    properties.setProperty(getKeyFor(column, TEXT_INDEX_TYPE), TextIndexType.LUCENE.name());
    properties.save();
  }

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer buffer = _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    return new VarByteChunkSingleValueReader(buffer);
  }
}
