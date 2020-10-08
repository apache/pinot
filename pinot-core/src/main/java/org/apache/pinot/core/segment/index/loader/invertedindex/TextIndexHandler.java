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
import org.apache.pinot.core.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.core.segment.index.readers.forward.BaseChunkSVForwardIndexReader.ChunkReaderContext;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.sorted.SortedIndexReaderImpl;
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
   * Right now the text index is supported on RAW and dictionary encoded
   * single-value STRING columns. Later we can add support for text index
   * on multi-value columns and BYTE type columns
   * @param columnMetadata metadata for column
   */
  private void checkUnsupportedOperationsForTextIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.getDataType() != DataType.STRING) {
      throw new UnsupportedOperationException("Text index is currently only supported on STRING columns: " + column);
    }
    if (!columnMetadata.isSingleValue()) {
      throw new UnsupportedOperationException(
          "Text index is currently not supported on multi-value columns: " + column);
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
    boolean hasDictionary = columnMetadata.hasDictionary();
    LOGGER.info("Creating new text index for column: {} in segment: {}, hasDictionary: {}", column, _segmentName,
        hasDictionary);
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, _segmentVersion);
    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        LuceneTextIndexCreator textIndexCreator = new LuceneTextIndexCreator(column, segmentDirectory, true)) {
      if (!hasDictionary) {
        // text index on raw column, just read the raw forward index
        VarByteChunkSVForwardIndexReader rawIndexReader = (VarByteChunkSVForwardIndexReader) forwardIndexReader;
        ChunkReaderContext chunkReaderContext = (ChunkReaderContext) readerContext;
        for (int docId = 0; docId < numDocs; docId++) {
          textIndexCreator.add(rawIndexReader.getString(docId, chunkReaderContext));
        }
      } else {
        // text index on dictionary encoded SV column
        // read forward index to get dictId
        // read the raw value from dictionary using dictId
        try (BaseImmutableDictionary dictionary = getDictionaryReader(columnMetadata)) {
          for (int docId = 0; docId < numDocs; docId++) {
            int dictId = forwardIndexReader.getDictId(docId, readerContext);
            textIndexCreator.add(dictionary.getStringValue(dictId));
          }
        }
      }
      textIndexCreator.seal();
    }

    LOGGER.info("Created text index for column: {} in segment: {}", column, _segmentName);
    PropertiesConfiguration properties = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    properties.setProperty(getKeyFor(column, TEXT_INDEX_TYPE), TextIndexType.LUCENE.name());
    properties.save();
  }

  private ForwardIndexReader<?> getForwardIndexReader(ColumnMetadata columnMetadata)
      throws IOException {
    if (!columnMetadata.hasDictionary()) {
      // text index on raw column
      PinotDataBuffer buffer =
          _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
      return new VarByteChunkSVForwardIndexReader(buffer, DataType.STRING);
    } else {
      // text index on dictionary encoded column
      // two cases:
      // 1. column is sorted
      // 2. column is unsorted
      PinotDataBuffer buffer =
          _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
      int numRows = columnMetadata.getTotalDocs();
      int numBitsPerValue = columnMetadata.getBitsPerElement();
      if (columnMetadata.isSorted()) {
        // created sorted dictionary based forward index reader
        return new SortedIndexReaderImpl(buffer, columnMetadata.getCardinality());
      } else {
        // create bit-encoded dictionary based forward index reader
        return new FixedBitSVForwardIndexReader(buffer, numRows, numBitsPerValue);
      }
    }
  }

  private BaseImmutableDictionary getDictionaryReader(ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dictionaryBuffer =
        _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    return new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(), columnMetadata.getColumnMaxLength(),
        (byte) columnMetadata.getPaddingCharacter());
  }
}
