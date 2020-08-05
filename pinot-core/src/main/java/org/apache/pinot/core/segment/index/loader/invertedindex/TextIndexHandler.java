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
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.TextIndexType;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.forward.BaseChunkSVForwardIndexReader.ChunkReaderContext;
import org.apache.pinot.core.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.TEXT_INDEX_TYPE;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Column.getKeyFor;


/**
 * Helper class for text indexes used by {@link org.apache.pinot.core.segment.index.loader.SegmentPreProcessor}.
 * to create text index for column during segment load time. Currently text index is always
 * created (if enabled on a column) during segment generation
 *
 * (1) A new segment with text index is created/refreshed. Server loads the segment. The handler
 * detects the existence of text index and returns.
 *
 * (2) A reload is issued on an existing segment with existing text index. The handler
 * detects the existence of text index and returns.
 *
 * (3) A reload is issued on an existing segment after text index is enabled on an existing
 * column. Read the forward index to create text index.
 *
 * (4) A reload is issued on an existing segment after text index is enabled on a newly
 * added column. In this case, the default column handler would have taken care of adding
 * forward index for the new column. Read the forward index to create text index.
 */
public class TextIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _textIndexColumns = new HashSet<>();

  public TextIndexHandler(File indexDir, SegmentMetadataImpl segmentMetadata, Set<String> textIndexColumns,
      SegmentDirectory.Writer segmentWriter) {
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
      throw new UnsupportedOperationException(
          "Text index is currently not supported on dictionary encoded column: " + column);
    }

    if (columnMetadata.isSorted()) {
      // since Pinot's current implementation doesn't support raw sorted columns,
      // we need to check for this too
      throw new UnsupportedOperationException("Text index is currently not supported on sorted columns: " + column);
    }

    if (!columnMetadata.isSingleValue()) {
      throw new UnsupportedOperationException(
          "Text index is currently not supported on multi-value columns: " + column);
    }

    if (columnMetadata.getDataType() != DataType.STRING) {
      throw new UnsupportedOperationException("Text index is currently only supported on STRING columns: " + column);
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
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, _segmentVersion);
    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.
    try (LuceneTextIndexCreator textIndexCreator = new LuceneTextIndexCreator(column, segmentDirectory, true);
        VarByteChunkSVForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata);
        ChunkReaderContext readerContext = forwardIndexReader.createContext()) {
      for (int docId = 0; docId < numDocs; docId++) {
        textIndexCreator.addDoc(forwardIndexReader.getString(docId, readerContext), docId);
      }
      textIndexCreator.seal();
    }
    LOGGER.info("Created text index for column: {} in segment: {}", column, _segmentName);
    PropertiesConfiguration properties = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    properties.setProperty(getKeyFor(column, TEXT_INDEX_TYPE), TextIndexType.LUCENE.name());
    properties.save();
  }

  private VarByteChunkSVForwardIndexReader getForwardIndexReader(ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer buffer = _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    return new VarByteChunkSVForwardIndexReader(buffer, DataType.STRING);
  }
}
