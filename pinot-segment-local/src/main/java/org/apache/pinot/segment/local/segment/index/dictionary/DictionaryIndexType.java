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

package org.apache.pinot.segment.local.segment.index.dictionary;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.BigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapBigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapBytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapDoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapFloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapIntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapLongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapStringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DictionaryIndexType
    implements IndexType<DictionaryIndexConfig, Dictionary, SegmentDictionaryCreator>,
               ConfigurableFromIndexLoadingConfig<DictionaryIndexConfig> {
  public static final DictionaryIndexType INSTANCE = new DictionaryIndexType();
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryIndexType.class);

  private DictionaryIndexType() {
  }

  @Override
  public String getId() {
    return "dictionary";
  }

  @Override
  public String getIndexName() {
    return "dictionary";
  }

  @Override
  public Class<DictionaryIndexConfig> getIndexConfigClass() {
    return DictionaryIndexConfig.class;
  }

  @Nullable
  @Override
  public DictionaryIndexConfig getDefaultConfig() {
    return DictionaryIndexConfig.DEFAULT;
  }

  @Override
  public Map<String, IndexDeclaration<DictionaryIndexConfig>> fromIndexLoadingConfig(
      IndexLoadingConfig indexLoadingConfig) {
    Map<String, IndexDeclaration<DictionaryIndexConfig>> result = new HashMap<>();
    Set<String> noDictionaryColumns = indexLoadingConfig.getNoDictionaryColumns();
    Set<String> onHeapCols = indexLoadingConfig.getOnHeapDictionaryColumns();
    Set<String> varLengthCols = indexLoadingConfig.getVarLengthDictionaryColumns();
    for (String column : indexLoadingConfig.getAllKnownColumns()) {
      if (!noDictionaryColumns.contains(column)) {
        DictionaryIndexConfig conf =
            new DictionaryIndexConfig(onHeapCols.contains(column), varLengthCols.contains(column));
        result.put(column, IndexDeclaration.declared(conf));
      } else {
        result.put(column, IndexDeclaration.declaredDisabled());
      }
    }
    return result;
  }

  @Override
  public IndexDeclaration<DictionaryIndexConfig> deserializeSpreadConf(
      TableConfig tableConfig, Schema schema, String column) {
    IndexingConfig ic = tableConfig.getIndexingConfig();
    boolean isNoDictionary = ic.getNoDictionaryColumns() != null && ic.getNoDictionaryColumns().contains(column);
    if (isNoDictionary) {
      return IndexDeclaration.declaredDisabled();
    }
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList != null) {
      Optional<FieldConfig> fieldConfig = fieldConfigList.stream().filter(fc -> fc.getName().equals(column)).findAny();
      if (fieldConfig.isPresent() && fieldConfig.get().getEncodingType() == FieldConfig.EncodingType.RAW) {
        return IndexDeclaration.declaredDisabled();
      }
    }
    boolean isOnHeap = ic.getOnHeapDictionaryColumns() != null && ic.getOnHeapDictionaryColumns().contains(column);
    boolean isVarLength = ic.getVarLengthDictionaryColumns() != null
        && ic.getVarLengthDictionaryColumns().contains(column);

    return IndexDeclaration.declared(new DictionaryIndexConfig(isOnHeap, isVarLength));
  }

  @Override
  public boolean shouldBeCreated(IndexCreationContext context, FieldIndexConfigs configs) {
    return context.hasDictionary();
  }

  @Override
  public SegmentDictionaryCreator createIndexCreator(IndexCreationContext context, DictionaryIndexConfig indexConfig) {
    boolean useVarLengthDictionary = shouldUseVarLengthDictionary(context, indexConfig);
    return new SegmentDictionaryCreator(context.getFieldSpec(), context.getIndexDir(), useVarLengthDictionary);
  }

  public boolean shouldUseVarLengthDictionary(IndexCreationContext context, DictionaryIndexConfig indexConfig) {
    if (indexConfig.getUseVarLengthDictionary()) {
      return true;
    }
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    if (storedType != FieldSpec.DataType.BYTES && storedType != FieldSpec.DataType.BIG_DECIMAL) {
      return false;
    }
    return !context.isFixedLength();
  }

  public static boolean shouldUseVarLengthDictionary(String columnName, Set<String> varLengthDictColumns,
      FieldSpec.DataType columnStoredType, ColumnStatistics columnProfile) {
    if (varLengthDictColumns.contains(columnName)) {
      return true;
    }

    return shouldUseVarLengthDictionary(columnStoredType, columnProfile);
  }

  public static boolean shouldUseVarLengthDictionary(FieldSpec.DataType columnStoredType, ColumnStatistics profile) {
    if (columnStoredType == FieldSpec.DataType.BYTES || columnStoredType == FieldSpec.DataType.BIG_DECIMAL) {
      return !profile.isFixedLength();
    }

    return false;
  }

  public SegmentDictionaryCreator createIndexCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary)
      throws Exception {
    return new SegmentDictionaryCreator(fieldSpec, indexDir, useVarLengthDictionary);
  }

  public Dictionary read(SegmentDirectory.Reader segmentReader, ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dataBuffer = segmentReader.getIndexFor(columnMetadata.getColumnName(), this);
    return read(dataBuffer, columnMetadata, new DictionaryIndexConfig(false, true));
  }

  public Dictionary read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, DictionaryIndexConfig indexConfig)
      throws IOException {
    FieldSpec.DataType dataType = metadata.getDataType();
    boolean loadOnHeap = indexConfig.isOnHeap();
    if (loadOnHeap) {
      String columnName = metadata.getColumnName();
      LOGGER.info("Loading on-heap dictionary for column: {}", columnName);
    }

    int length = metadata.getCardinality();
    switch (dataType.getStoredType()) {
      case INT:
        return loadOnHeap ? new OnHeapIntDictionary(dataBuffer, length)
            : new IntDictionary(dataBuffer, length);
      case LONG:
        return loadOnHeap ? new OnHeapLongDictionary(dataBuffer, length)
            : new LongDictionary(dataBuffer, length);
      case FLOAT:
        return loadOnHeap ? new OnHeapFloatDictionary(dataBuffer, length)
            : new FloatDictionary(dataBuffer, length);
      case DOUBLE:
        return loadOnHeap ? new OnHeapDoubleDictionary(dataBuffer, length)
            : new DoubleDictionary(dataBuffer, length);
      case BIG_DECIMAL:
        int numBytesPerValue = metadata.getColumnMaxLength();
        return loadOnHeap ? new OnHeapBigDecimalDictionary(dataBuffer, length, numBytesPerValue)
            : new BigDecimalDictionary(dataBuffer, length, numBytesPerValue);
      case STRING:
        numBytesPerValue = metadata.getColumnMaxLength();
        return loadOnHeap ? new OnHeapStringDictionary(dataBuffer, length, numBytesPerValue)
            : new StringDictionary(dataBuffer, length, numBytesPerValue);
      case BYTES:
        numBytesPerValue = metadata.getColumnMaxLength();
        return loadOnHeap ? new OnHeapBytesDictionary(dataBuffer, length, numBytesPerValue)
            : new BytesDictionary(dataBuffer, length, numBytesPerValue);
      default:
        throw new IllegalStateException("Unsupported data type for dictionary: " + dataType);
    }
  }

  @Override
  public IndexReaderFactory<Dictionary> getReaderFactory() {
    return new IndexReaderFactory.Default<DictionaryIndexConfig, Dictionary>() {
      @Override
      protected IndexType<DictionaryIndexConfig, Dictionary, ?> getIndexType() {
        return DictionaryIndexType.INSTANCE;
      }

      @Override
      protected Dictionary read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, DictionaryIndexConfig indexConfig,
          File segmentDir)
          throws IOException, IndexReaderConstraintException {
        return DictionaryIndexType.INSTANCE.read(dataBuffer, metadata, indexConfig);
      }
    };
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new IndexHandler() {
      @Override
      public void updateIndices(SegmentDirectory.Writer segmentWriter) {
      }

      @Override
      public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
        return false;
      }

      @Override
      public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter) {
      }
    };
  }

  public String getFileExtension() {
    return V1Constants.Dict.FILE_EXTENSION;
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return getFileExtension();
  }

  @Override
  public String toString() {
    return getId();
  }
}
