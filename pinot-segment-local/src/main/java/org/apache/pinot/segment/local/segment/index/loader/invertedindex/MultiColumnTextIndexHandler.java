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
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.creator.impl.text.MultiColumnLuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
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
 *
 * NOTE:
 * Class is based on TextIndexHandler but operates on to multi-column text index.
 * This class extends BaseIndexHandler only to reuse some functionality (temporary forward indices handling)
 * but should not be used in the same way as other single-column indexes.
 * As the name implies, multi-column text index is not bound to a single column and can't be built with column-major
 * approach.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultiColumnTextIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiColumnTextIndexHandler.class);

  private final MultiColumnTextIndexConfig _textIndexConfig;

  public MultiColumnTextIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig,
      MultiColumnTextIndexConfig textIndexConfig) {
    super(segmentDirectory, indexLoadingConfig);
    _textIndexConfig = textIndexConfig;
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    MultiColumnTextMetadata oldConfig = segmentMetadata.getMultiColumnTextMetadata();

    boolean needUpdate = shouldModifyMultiColTextIndex(_textIndexConfig, oldConfig);
    if (needUpdate) {
      List<String> newColumns = _textIndexConfig.getColumns();
      TableConfigUtils.checkForDuplicates(newColumns);
      for (String column : newColumns) {
        ColumnMetadata columnMeta = segmentMetadata.getColumnMetadataFor(column);
        if (columnMeta != null) {
          validate(columnMeta.getDataType(), column);
        }
      }

      LOGGER.info("Segment: {} does require (re)building multi column text index on columns: {}", segmentName,
          _textIndexConfig.getColumns());

      return true;
    } else {
      LOGGER.info("Segment: {} does not required (re)building multi column text index", segmentName);
      return false;
    }
  }

  private void validate(DataType columnMeta, String column) {
    if (columnMeta != DataType.STRING) {
      throw new UnsupportedOperationException(
          "Cannot create TEXT index on column: " + column + " of stored type other than STRING");
    }

    TextIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.text());
    if (config != null && config.isEnabled()) {
      throw new UnsupportedOperationException(
          "Cannot create both single and multi-column TEXT index on column: " + column);
    }
  }

  public static boolean shouldModifyMultiColTextIndex(MultiColumnTextIndexConfig newConfig,
      MultiColumnTextMetadata oldConfig) {
    if (newConfig == null) {
      return oldConfig != null;
    } else {
      if (oldConfig == null) {
        return true;
      }

      if (!oldConfig.getColumns().equals(newConfig.getColumns())) {
        return true;
      }

      Map<String, String> newProperties = newConfig.getProperties();
      if (!MultiColumnTextMetadata.equalsSharedProps(oldConfig.getSharedProperties(), newProperties)) {
        return true;
      }

      return !MultiColumnTextMetadata.equalsColumnProps(
          newConfig.getPerColumnProperties(),
          oldConfig.getPerColumnProperties());
    }
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    TableConfigUtils.checkForDuplicates(_textIndexConfig.getColumns());

    BooleanList columnsSV = new BooleanArrayList(_textIndexConfig.getColumns().size());
    for (String column : _textIndexConfig.getColumns()) {
      ColumnMetadata metadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      validate(metadata.getFieldSpec().getDataType(), column);
      columnsSV.add(metadata.isSingleValue());
    }

    try (MultiColumnLuceneTextIndexCreator creator =
        new MultiColumnLuceneTextIndexCreator(_textIndexConfig.getColumns(), columnsSV, segmentDirectory, true, false,
            null, null, _textIndexConfig)) {
      createMultiColumnTextIndex(segmentWriter, creator);
      creator.seal();

      // Write the metadata for multi-column text index
      PropertiesConfiguration metadataProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
      MultiColumnTextMetadata.writeMetadata(metadataProperties, MultiColumnTextMetadata.VERSION_1,
          _textIndexConfig.getColumns(), _textIndexConfig.getProperties(), _textIndexConfig.getPerColumnProperties());
      SegmentMetadataUtils.savePropertiesConfiguration(metadataProperties, indexDir);
    }
  }

  private void createMultiColumnTextIndex(SegmentDirectory.Writer segmentWriter,
      MultiColumnLuceneTextIndexCreator textIndexCreator)
      throws IOException, IndexReaderConstraintException {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    int numDocs = segmentMetadata.getTotalDocs();

    List<String> indexedColumns = _textIndexConfig.getColumns();
    List<ForwardIndexReader> fwdReaders = new ArrayList<>(indexedColumns.size());
    List<ForwardIndexReaderContext> fwdReaderContexts = new ArrayList<>(indexedColumns.size());
    List<Dictionary> dictionaries = new ArrayList<>(indexedColumns.size());
    List<String[]> valueBuffers = new ArrayList<>(indexedColumns.size());
    List<int[]> dictIdBuffers = new ArrayList<>(indexedColumns.size());
    int[] lengths = new int[indexedColumns.size()];
    boolean[] hasDictionary = new boolean[indexedColumns.size()];
    boolean[] isSingleValue = new boolean[indexedColumns.size()];

    for (int i = 0, n = indexedColumns.size(); i < n; i++) {
      String columnName = indexedColumns.get(i);
      ColumnMetadata metadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);
      IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
      ForwardIndexReader fwdReader =
          readerFactory.createIndexReader(segmentWriter, _fieldIndexConfigs.get(metadata.getColumnName()), metadata);
      fwdReaders.add(fwdReader);
      fwdReaderContexts.add(fwdReader.createContext());

      hasDictionary[i] = metadata.hasDictionary();
      isSingleValue[i] = metadata.isSingleValue();

      if (metadata.hasDictionary()) {
        dictionaries.add(DictionaryIndexType.read(segmentWriter, metadata));
      } else {
        dictionaries.add(null);
      }

      if (metadata.isSingleValue()) {
        valueBuffers.add(null);
        dictIdBuffers.add(null);
      } else {
        valueBuffers.add(new String[metadata.getMaxNumberOfMultiValues()]);
        if (metadata.hasDictionary()) {
          dictIdBuffers.add(new int[metadata.getMaxNumberOfMultiValues()]);
        } else {
          dictIdBuffers.add(null);
        }
      }

      LOGGER.info("Including column: {} in multi-column text index in segment: {}, hasDictionary: {}", columnName,
          segmentName, hasDictionary[i]);
    }

    try {
      for (int docId = 0; docId < numDocs; docId++) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < indexedColumns.size(); i++) {
          ForwardIndexReader forwardReader = fwdReaders.get(i);
          ForwardIndexReaderContext readerContext = fwdReaderContexts.get(i);

          if (isSingleValue[i]) {
            if (!hasDictionary[i]) {
              // text index on raw column, just read the raw forward index
              values.add(forwardReader.getString(docId, readerContext));
            } else {
              // text index on dictionary encoded SV column
              // read forward index to get dictId
              // read the raw value from dictionary using dictId
              int dictId = forwardReader.getDictId(docId, readerContext);
              values.add(dictionaries.get(i).getStringValue(dictId));
            }
          } else {
            if (!hasDictionary[i]) {
              // text index on raw column, just read the raw forward index
              // read forward index to get multi-value
              // read the raw value from forward index using docId
              String[] valueBuffer = valueBuffers.get(i);
              int length = forwardReader.getStringMV(docId, valueBuffer, readerContext);
              lengths[i] = length;
              values.add(valueBuffer);
            } else {
              // text index on dictionary encoded MV column
              // read forward index to get dictId
              // read the raw value from dictionary using dictId
              int[] dictIdBuffer = dictIdBuffers.get(i);
              int length = forwardReader.getDictIdMV(docId, dictIdBuffer, readerContext);
              lengths[i] = length;
              String[] valueBuffer = valueBuffers.get(i);
              for (int j = 0; j < length; j++) {
                valueBuffer[j] = dictionaries.get(i).getStringValue(dictIdBuffer[j]);
              }
              values.add(valueBuffer);
            }
          }
        }
        textIndexCreator.add(values, lengths);
      }

      LOGGER.info("Finished adding multi-column text index to segment: {}", segmentName);
    } finally {
      for (int i = 0; i < fwdReaders.size(); i++) {
        try {
          ForwardIndexReader fwdReader = fwdReaders.get(i);
          ForwardIndexReaderContext readerContext = fwdReaderContexts.get(i);
          fwdReader.close();
          if (readerContext != null) {
            readerContext.close();
          }
          if (dictionaries.get(i) != null) {
            dictionaries.get(i).close();
          }
        } catch (Exception e) {
          LOGGER.error("Failed to close forward index reader for column: {}", _textIndexConfig.getColumns().get(i), e);
        }
      }
    }
  }
}
