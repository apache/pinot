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
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class TextIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  public TextIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.text(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.text());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing text index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateTextIndex(columnMetadata)) {
        LOGGER.info("Need to create new text index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.text());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing text index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.text());
        LOGGER.info("Removed existing text index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateTextIndex(columnMetadata)) {
        createTextIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private boolean shouldCreateTextIndex(ColumnMetadata columnMetadata) {
    if (columnMetadata != null) {
      // Fail fast upon unsupported operations.
      checkUnsupportedOperationsForTextIndex(columnMetadata);
      return true;
    }
    return false;
  }

  /**
   * Right now the text index is supported on STRING columns.
   * Later we can add support for text index on BYTES columns
   * @param columnMetadata metadata for column
   */
  private void checkUnsupportedOperationsForTextIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.getDataType() != DataType.STRING) {
      throw new UnsupportedOperationException("Text index is currently only supported on STRING columns: " + column);
    }
  }

  private void createTextIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    int numDocs = columnMetadata.getTotalDocs();
    boolean hasDictionary = columnMetadata.hasDictionary();

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    LOGGER.info("Creating new text index for column: {} in segment: {}, hasDictionary: {}", columnName, segmentName,
        hasDictionary);
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.

    IndexCreationContext context = IndexCreationContext.builder()
        .withColumnMetadata(columnMetadata)
        .withIndexDir(segmentDirectory)
        .withTextCommitOnClose(true)
        .build();
    TextIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.text());

    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        TextIndexCreator textIndexCreator = StandardIndexes.text().createIndexCreator(context, config)) {
      if (columnMetadata.isSingleValue()) {
        processSVField(segmentWriter, hasDictionary, forwardIndexReader, readerContext, textIndexCreator, numDocs,
            columnMetadata);
      } else {
        processMVField(segmentWriter, hasDictionary, forwardIndexReader, readerContext, textIndexCreator, numDocs,
            columnMetadata);
      }
      textIndexCreator.seal();
    }

    LOGGER.info("Created text index for column: {} in segment: {}", columnName, segmentName);
  }

  private void processSVField(SegmentDirectory.Writer segmentWriter, boolean hasDictionary,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, TextIndexCreator textIndexCreator,
      int numDocs, ColumnMetadata columnMetadata)
      throws IOException {
    if (!hasDictionary) {
      // text index on raw column, just read the raw forward index
      for (int docId = 0; docId < numDocs; docId++) {
        textIndexCreator.add(forwardIndexReader.getString(docId, readerContext));
      }
    } else {
      // text index on dictionary encoded SV column
      // read forward index to get dictId
      // read the raw value from dictionary using dictId
      try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
        for (int docId = 0; docId < numDocs; docId++) {
          int dictId = forwardIndexReader.getDictId(docId, readerContext);
          textIndexCreator.add(dictionary.getStringValue(dictId));
        }
      }
    }
  }

  private void processMVField(SegmentDirectory.Writer segmentWriter, boolean hasDictionary,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, TextIndexCreator textIndexCreator,
      int numDocs, ColumnMetadata columnMetadata)
      throws IOException {
    if (!hasDictionary) {
      // text index on raw column, just read the raw forward index
      String[] valueBuffer = new String[columnMetadata.getMaxNumberOfMultiValues()];
      for (int docId = 0; docId < numDocs; docId++) {
        int length = forwardIndexReader.getStringMV(docId, valueBuffer, readerContext);
        textIndexCreator.add(valueBuffer, length);
      }
    } else {
      // text index on dictionary encoded MV column
      // read forward index to get dictId
      // read the raw value from dictionary using dictId
      try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
        int maxNumEntries = columnMetadata.getMaxNumberOfMultiValues();
        int[] dictIdBuffer = new int[maxNumEntries];
        String[] valueBuffer = new String[maxNumEntries];
        for (int docId = 0; docId < numDocs; docId++) {
          int length = forwardIndexReader.getDictIdMV(docId, dictIdBuffer, readerContext);
          for (int i = 0; i < length; i++) {
            valueBuffer[i] = dictionary.getStringValue(dictIdBuffer[i]);
          }
          textIndexCreator.add(valueBuffer, length);
        }
      }
    }
  }
}
