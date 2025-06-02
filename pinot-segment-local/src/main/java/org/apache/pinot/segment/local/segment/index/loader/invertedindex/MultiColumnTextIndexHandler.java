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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.creator.impl.text.MultiColumnLuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for text indexes used by {@link SegmentPreProcessor}.
 * to create text index for column during segment load time. Currently, text index is always
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
public class MultiColumnTextIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiColumnTextIndexHandler.class);

  private final MultiColumnTextIndexConfig _textIndexConfig;

  public MultiColumnTextIndexHandler(
      SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> fieldIndexConfigs,
      MultiColumnTextIndexConfig textIndexConfig,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _textIndexConfig = textIndexConfig;
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    MultiColumnTextMetadata multiColumnTextMetadata = segmentMetadata.getMultiColumnTextMetadata();
    if (multiColumnTextMetadata == null) {
      // If the segment does not have multi-column text index metadata, we need to create it.
      LOGGER.info("Segment: {} does not have multi-column text index metadata, need to create it", segmentName);
      return true;
    }
    // If the segment has multi-column text index metadata, check if it needs to be updated.
    List<String> existingColumns = multiColumnTextMetadata.getColumns();
    List<String> newColumns = _textIndexConfig.getColumns();
    if (existingColumns.equals(newColumns)) {
      // If the existing columns are the same as the new columns, no need to update.
      LOGGER.info("Segment: {} already has multi-column text index for columns: {}, no need to update",
          segmentName, existingColumns);
      return false;
    }
    for (String column : newColumns) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        // Fail fast upon unsupported operations.
        if (columnMetadata.getDataType() != DataType.STRING) {
          throw new UnsupportedOperationException(
              "Text index is currently only supported on STRING columns: " + column);
        }
      }
    }
    return true;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    BooleanList columnsSV = new BooleanArrayList(_textIndexConfig.getColumns().size());
    for (String column : _textIndexConfig.getColumns()) {
      ColumnMetadata metadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      columnsSV.add(metadata.isSingleValue());
    }

    //TODO: check actual arguments
    try (MultiColumnLuceneTextIndexCreator creator =
        new MultiColumnLuceneTextIndexCreator(_textIndexConfig.getColumns(), columnsSV, segmentDirectory, true, false,
            null, null, _textIndexConfig)) {
      createMultiColumnTextIndices(segmentWriter, creator);
      creator.seal();

      // Write the metadata for multi-column text index
      PropertiesConfiguration metadataProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
      MultiColumnTextMetadata.writeMetadata(metadataProperties, MultiColumnTextMetadata.VERSION_1,
          _textIndexConfig.getColumns(), _textIndexConfig.getProperties(), _textIndexConfig.getPerColumnProperties());
      SegmentMetadataUtils.savePropertiesConfiguration(metadataProperties, indexDir);
    }
  }

  private void createMultiColumnTextIndices(SegmentDirectory.Writer segmentWriter,
      MultiColumnLuceneTextIndexCreator textIndexCreator)
      throws IOException {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    int numDocs = segmentMetadata.getTotalDocs();

    List<String> textIndexConfigColumns = _textIndexConfig.getColumns();
    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    List<ForwardIndexReader> forwardIndexReaderList = new ArrayList<>();
    List<ForwardIndexReaderContext> forwardIndexReaderContextList = new ArrayList<>();
    for (String columnName : textIndexConfigColumns) {
      // Create a temporary forward index if it is disabled and does not exist
      ColumnMetadata columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);
      columnMetadataList.add(columnMetadata);
      ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
      forwardIndexReaderList.add(forwardIndexReader);
      forwardIndexReaderContextList.add(forwardIndexReader.createContext());
      boolean hasDictionary = columnMetadata.hasDictionary();
      LOGGER.info("Adding column: {} to multi-column text index in segment: {}, hasDictionary: {}", columnName,
          segmentName, hasDictionary);
    }

    for (int docId = 0; docId < numDocs; docId++) {
      List<Object> documentValues = new ArrayList<>();
      for (int i = 0; i < textIndexConfigColumns.size(); i++) {
        ForwardIndexReader forwardIndexReader = forwardIndexReaderList.get(i);
        ForwardIndexReaderContext readerContext = forwardIndexReaderContextList.get(i);

        ColumnMetadata columnMetadata = columnMetadataList.get(i);
        boolean hasDictionary = columnMetadata.hasDictionary();
        if (columnMetadata.isSingleValue()) {
          if (!hasDictionary) {
            // text index on raw column, just read the raw forward index
            documentValues.add(forwardIndexReader.getString(docId, readerContext));
          } else {
            // text index on dictionary encoded SV column
            // read forward index to get dictId
            // read the raw value from dictionary using dictId
            try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
              int dictId = forwardIndexReader.getDictId(docId, readerContext);
              documentValues.add(dictionary.getStringValue(dictId));
            }
          }
        } else {
          if (!hasDictionary) {
            // text index on raw column, just read the raw forward index
            // read forward index to get multi-value
            // read the raw value from forward index using docId
            // add all values to documentValues

            String[] valueBuffer = new String[columnMetadata.getMaxNumberOfMultiValues()];
            int length = forwardIndexReader.getStringMV(docId, valueBuffer, readerContext);
            documentValues.add(Arrays.copyOf(valueBuffer, length));
          } else {
            // text index on dictionary encoded MV column
            // read forward index to get dictId
            // read the raw value from dictionary using dictId
            try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
              int maxNumEntries = columnMetadata.getMaxNumberOfMultiValues();
              int[] dictIdBuffer = new int[maxNumEntries];
              int length = forwardIndexReader.getDictIdMV(docId, dictIdBuffer, readerContext);
              String[] valueBuffer = new String[length];
              for (int j = 0; j < length; j++) {
                valueBuffer[j] = dictionary.getStringValue(dictIdBuffer[j]);
              }
              documentValues.add(valueBuffer);
            }
          }
        }
      }
      textIndexCreator.add(documentValues);
    }

    for (int i = 0; i < _textIndexConfig.getColumns().size(); i++) {
      try {
        ForwardIndexReader forwardIndexReader = forwardIndexReaderList.get(i);
        ForwardIndexReaderContext readerContext = forwardIndexReaderContextList.get(i);
        forwardIndexReader.close();
        if (readerContext != null) {
          readerContext.close();
        }
      } catch (Exception e) {
        LOGGER.error("Failed to close forward index reader for column: {}", _textIndexConfig.getColumns().get(i), e);
      }
    }
  }
}
