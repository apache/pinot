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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.index.readers.forward.BaseChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.creator.TextIndexType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.TEXT_INDEX_TYPE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;


/**
 * Helper class for text indexes used by {@link SegmentPreProcessor}.
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
@SuppressWarnings({"rawtypes", "unchecked"})
public class TextIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexHandler.class);

  private final File _indexDir;
  private final SegmentMetadata _segmentMetadata;
  private final SegmentDirectory.Writer _segmentWriter;
  private final Set<String> _columnsToAddIdx;

  public TextIndexHandler(File indexDir, SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _columnsToAddIdx = indexLoadingConfig.getTextIndexColumns();
  }

  @Override
  public void updateIndices()
      throws Exception {
    // Remove indices not set in table config any more
    Set<String> existingColumns = _segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.TEXT_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        _segmentWriter.removeIndex(column, ColumnIndexType.TEXT_INDEX);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        checkUnsupportedOperationsForTextIndex(columnMetadata);
        createTextIndexForColumn(columnMetadata);
      }
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
    String segmentName = _segmentMetadata.getName();
    String column = columnMetadata.getColumnName();
    if (_segmentWriter.hasIndexFor(column, ColumnIndexType.TEXT_INDEX)) {
      // Skip creating text index if already exists.
      LOGGER.info("Found text index for column: {}, in segment: {}", column, segmentName);
      return;
    }
    int numDocs = columnMetadata.getTotalDocs();
    boolean hasDictionary = columnMetadata.hasDictionary();
    LOGGER.info("Creating new text index for column: {} in segment: {}, hasDictionary: {}", column, segmentName,
        hasDictionary);
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, _segmentMetadata.getVersion());
    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(_segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        LuceneTextIndexCreator textIndexCreator = new LuceneTextIndexCreator(column, segmentDirectory, true)) {
      if (!hasDictionary) {
        // text index on raw column, just read the raw forward index
        VarByteChunkSVForwardIndexReader rawIndexReader = (VarByteChunkSVForwardIndexReader) forwardIndexReader;
        BaseChunkSVForwardIndexReader.ChunkReaderContext chunkReaderContext =
            (BaseChunkSVForwardIndexReader.ChunkReaderContext) readerContext;
        for (int docId = 0; docId < numDocs; docId++) {
          textIndexCreator.add(rawIndexReader.getString(docId, chunkReaderContext));
        }
      } else {
        // text index on dictionary encoded SV column
        // read forward index to get dictId
        // read the raw value from dictionary using dictId
        try (Dictionary dictionary = LoaderUtils.getDictionary(_segmentWriter, columnMetadata)) {
          for (int docId = 0; docId < numDocs; docId++) {
            int dictId = forwardIndexReader.getDictId(docId, readerContext);
            textIndexCreator.add(dictionary.getStringValue(dictId));
          }
        }
      }
      textIndexCreator.seal();
    }

    LOGGER.info("Created text index for column: {} in segment: {}", column, segmentName);
    PropertiesConfiguration properties = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    properties.setProperty(getKeyFor(column, TEXT_INDEX_TYPE), TextIndexType.LUCENE.name());
    properties.save();
  }
}
