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

package org.apache.pinot.segment.local.segment.index.forward;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.ForwardIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

import static org.apache.pinot.spi.data.FieldSpec.DataType.BIG_DECIMAL;
import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;


public class ForwardIndexType
    extends AbstractIndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<ForwardIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "forward";
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;

  protected ForwardIndexType() {
    super(StandardIndexes.FORWARD_ID);
  }

  @Override
  public Class<ForwardIndexConfig> getIndexConfigClass() {
    return ForwardIndexConfig.class;
  }

  @Override
  public Map<String, ForwardIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    Set<String> disabledCols = indexLoadingConfig.getForwardIndexDisabledColumns();
    Map<String, ForwardIndexConfig> result = new HashMap<>();
    Set<String> allColumns = Sets.union(disabledCols, indexLoadingConfig.getAllKnownColumns());
    for (String column : allColumns) {
      ChunkCompressionType compressionType =
          indexLoadingConfig.getCompressionConfigs() != null
              ? indexLoadingConfig.getCompressionConfigs().get(column)
              : null;
      Supplier<ForwardIndexConfig> defaultConfig = () -> {
        if (compressionType == null) {
          return ForwardIndexConfig.DEFAULT;
        } else {
          return new ForwardIndexConfig.Builder().withCompressionType(compressionType).build();
        }
      };
      if (!disabledCols.contains(column)) {
        TableConfig tableConfig = indexLoadingConfig.getTableConfig();
        if (tableConfig == null) {
          result.put(column, defaultConfig.get());
        } else {
          List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
          if (fieldConfigList == null) {
            result.put(column, defaultConfig.get());
            continue;
          }
          FieldConfig fieldConfig = fieldConfigList.stream()
              .filter(fc -> fc.getName().equals(column))
              .findAny()
              .orElse(null);
          if (fieldConfig == null) {
            result.put(column, defaultConfig.get());
            continue;
          }
          ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
          if (compressionType != null) {
            builder.withCompressionType(compressionType);
          } else {
            FieldConfig.CompressionCodec compressionCodec = fieldConfig.getCompressionCodec();
            if (compressionCodec != null) {
              builder.withCompressionType(ChunkCompressionType.valueOf(compressionCodec.name()));
            }
          }

          result.put(column, builder.build());
        }
      } else {
        result.put(column, ForwardIndexConfig.DISABLED);
      }
    }
    return result;
  }

  @Override
  public ForwardIndexConfig getDefaultConfig() {
    return ForwardIndexConfig.DEFAULT;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<ForwardIndexConfig> createDeserializer() {
    // reads tableConfig.fieldConfigList and decides what to create using the FieldConfig properties and encoding
    ColumnConfigDeserializer<ForwardIndexConfig> fromFieldConfig = IndexConfigDeserializer.fromCollection(
        TableConfig::getFieldConfigList,
        (accum, fieldConfig) -> {
          Map<String, String> properties = fieldConfig.getProperties();
          if (properties != null && isDisabled(properties)) {
            accum.put(fieldConfig.getName(), ForwardIndexConfig.DISABLED);
          } else if (fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW) {
            accum.put(fieldConfig.getName(), createConfigFromFieldConfig(fieldConfig));
          }
          // On other case encoding is DICTIONARY. We create the default forward index by default. That means that if
          // field config indicates for example a compression while using encoding dictionary, the compression is
          // ignored.
          // It is important to do not explicitly add the default value here in order to avoid exclusive problems with
          // the default `fromIndexes` deserializer.
        }
    );
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(fromFieldConfig);
  }

  private boolean isDisabled(Map<String, String> props) {
    return Boolean.parseBoolean(
        props.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
  }

  private ForwardIndexConfig createConfigFromFieldConfig(FieldConfig fieldConfig) {
    FieldConfig.CompressionCodec compressionCodec = fieldConfig.getCompressionCodec();
    ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
    if (compressionCodec != null) {
      builder.withCompressionType(ChunkCompressionType.valueOf(compressionCodec.name()));
    }

    Map<String, String> properties = fieldConfig.getProperties();
    if (properties != null) {
      builder.withLegacyProperties(properties);
    }

    return builder.build();
  }

  public static ChunkCompressionType getDefaultCompressionType(FieldSpec.FieldType fieldType) {
    if (fieldType == FieldSpec.FieldType.METRIC) {
      return ChunkCompressionType.PASS_THROUGH;
    } else {
      return ChunkCompressionType.LZ4;
    }
  }

  @Override
  public ForwardIndexCreator createIndexCreator(IndexCreationContext context, ForwardIndexConfig indexConfig)
      throws Exception {
    return ForwardIndexCreatorFactory.createIndexCreator(context, indexConfig);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new ForwardIndexHandler(segmentDirectory, configsByCol, schema, tableConfig);
  }

  @Override
  protected IndexReaderFactory<ForwardIndexReader> createReaderFactory() {
    return ForwardIndexReaderFactory.INSTANCE;
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    if (columnMetadata.isSingleValue()) {
      if (!columnMetadata.hasDictionary()) {
        return V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else if (columnMetadata.isSorted()) {
        return V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        return V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else if (!columnMetadata.hasDictionary()) {
      return V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      return V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will return the default reader, skipping any index overload.
   */
  public static ForwardIndexReader<?> read(SegmentDirectory.Reader segmentReader,
      ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dataBuffer = segmentReader.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.forward());
    return read(dataBuffer, columnMetadata);
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will return the default reader, skipping any index overload.
   */
  public static ForwardIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    return ForwardIndexReaderFactory.createIndexReader(dataBuffer, metadata);
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will delegate on {@link StandardIndexes}, so the correct reader will be returned even when using
   * index overload.
   */
  public static ForwardIndexReader<?> read(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
      ColumnMetadata metadata)
      throws IndexReaderConstraintException, IOException {
    return StandardIndexes.forward().getReaderFactory().createIndexReader(segmentReader, fieldIndexConfigs, metadata);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, ForwardIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    String column = context.getFieldSpec().getName();
    String segmentName = context.getSegmentName();
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    int fixedLengthBytes = context.getFixedLengthBytes();
    boolean isSingleValue = context.getFieldSpec().isSingleValueField();
    if (!context.hasDictionary()) {
      if (isSingleValue) {
        String allocationContext = IndexUtil.buildAllocationContext(context.getSegmentName(),
            context.getFieldSpec().getName(), V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (fixedLengthBytes > 0) {
          return new FixedByteSVMutableForwardIndex(false, storedType, fixedLengthBytes, context.getCapacity(),
              context.getMemoryManager(), allocationContext);
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity = Math.min(context.getCapacity(),
              NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          return new VarByteSVMutableForwardIndex(storedType, context.getMemoryManager(), allocationContext,
              initialCapacity, NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
        }
      } else {
        // TODO: Add support for variable width (bytes, string, big decimal) MV RAW column types
        assert storedType.isFixedWidth();
        String allocationContext =
            IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), storedType.size(), context.getMemoryManager(), allocationContext, false,
            storedType);
      }
    } else {
      if (isSingleValue) {
        String allocationContext = IndexUtil.buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        return new FixedByteSVMutableForwardIndex(true, FieldSpec.DataType.INT, context.getCapacity(),
            context.getMemoryManager(), allocationContext);
      } else {
        String allocationContext = IndexUtil.buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), Integer.BYTES, context.getMemoryManager(), allocationContext, true,
            FieldSpec.DataType.INT);
      }
    }
  }
}
