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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ForwardIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ForwardIndexType extends AbstractIndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexType.class);

  public static final String INDEX_DISPLAY_NAME = "forward";
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  public static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;
  //@formatter:off
  private static final List<String> EXTENSIONS = List.of(
      V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION
  );
  //@formatter:on

  protected ForwardIndexType() {
    super(StandardIndexes.FORWARD_ID);
  }

  @Override
  public Class<ForwardIndexConfig> getIndexConfigClass() {
    return ForwardIndexConfig.class;
  }

  @Override
  public ForwardIndexConfig getDefaultConfig() {
    return ForwardIndexConfig.getDefault();
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    ForwardIndexConfig forwardIndexConfig = indexConfigs.getConfig(StandardIndexes.forward());
    if (forwardIndexConfig.isEnabled()) {
      validateForwardIndexEnabled(forwardIndexConfig, indexConfigs, fieldSpec);
    } else {
      validateForwardIndexDisabled(indexConfigs, fieldSpec, tableConfig);
    }
  }

  private void validateForwardIndexEnabled(ForwardIndexConfig forwardIndexConfig, FieldIndexConfigs indexConfigs,
      FieldSpec fieldSpec) {
    String column = fieldSpec.getName();
    CompressionCodec compressionCodec = forwardIndexConfig.getCompressionCodec();
    DictionaryIndexConfig dictionaryConfig = indexConfigs.getConfig(StandardIndexes.dictionary());
    if (dictionaryConfig.isEnabled()) {
      Preconditions.checkState(compressionCodec == null || compressionCodec.isApplicableToDictEncodedIndex(),
          "Compression codec: %s is not applicable to dictionary encoded column: %s", compressionCodec, column);
    } else {
      boolean isCLPCodec = compressionCodec == CompressionCodec.CLP || compressionCodec == CompressionCodec.CLPV2
          || compressionCodec == CompressionCodec.CLPV2_ZSTD || compressionCodec == CompressionCodec.CLPV2_LZ4;
      if (isCLPCodec) {
        Preconditions.checkState(fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING,
            "Cannot apply CLP compression codec to column: %s of stored type other than STRING", column);
      } else {
        Preconditions.checkState(compressionCodec == null || compressionCodec.isApplicableToRawIndex(),
            "Compression codec: %s is not applicable to raw column: %s", compressionCodec, column);
      }
    }
  }

  private void validateForwardIndexDisabled(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec,
      TableConfig tableConfig) {
    String column = fieldSpec.getName();

    // TODO: Revisit this. We should allow dropping forward index after segment is sealed.
    Preconditions.checkState(tableConfig.getTableType() != TableType.REALTIME,
        "Cannot disable forward index for column: %s, as the table type is REALTIME", column);

    // Check for the range index since the index itself relies on the existence of the forward index to work.
    RangeIndexConfig rangeIndexConfig = indexConfigs.getConfig(StandardIndexes.range());
    if (rangeIndexConfig.isEnabled()) {
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          "Feature not supported for multi-value columns with range index. Cannot disable forward index for column: "
              + "%s. Disable range index on this column to use this feature.", column);
      Preconditions.checkState(rangeIndexConfig.getVersion() == BitSlicedRangeIndexCreator.VERSION,
          "Feature not supported for single-value columns with range index version < 2. Cannot disable forward index "
              + "for column: %s. Either disable range index or create range index with version >= 2 to use this "
              + "feature.", column);
    }

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    Preconditions.checkState(!indexingConfig.isOptimizeDictionaryForMetrics() && !indexingConfig.isOptimizeDictionary(),
        "Dictionary override optimization options (OptimizeDictionary, optimizeDictionaryForMetrics) not supported "
            + "with forward index disabled for column: %s", column);

    boolean hasDictionary = indexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled();
    boolean hasInvertedIndex = indexConfigs.getConfig(StandardIndexes.inverted()).isEnabled();
    if (!hasDictionary || !hasInvertedIndex) {
      LOGGER.warn("Forward index has been disabled for column: {}. Either dictionary ({}) and / or inverted index ({}) "
              + "has been disabled. If the forward index needs to be regenerated or another index added please refresh "
              + "or back-fill the forward index as it cannot be rebuilt without dictionary and inverted index.", column,
          hasDictionary ? "enabled" : "disabled", hasInvertedIndex ? "enabled" : "disabled");
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<ForwardIndexConfig> createDeserializerForLegacyConfigs() {
    // reads tableConfig.fieldConfigList and decides what to create using the FieldConfig properties and encoding
    return (tableConfig, schema) -> {
      Map<String, DictionaryIndexConfig> dictConfigs = StandardIndexes.dictionary().getConfig(tableConfig, schema);

      Map<String, ForwardIndexConfig> fwdConfig =
          Maps.newHashMapWithExpectedSize(Math.max(dictConfigs.size(), schema.size()));

      Collection<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          Map<String, String> properties = fieldConfig.getProperties();
          if (properties != null && isDisabled(properties)) {
            fwdConfig.put(fieldConfig.getName(), ForwardIndexConfig.getDisabled());
          } else {
            ForwardIndexConfig config = createConfigFromFieldConfig(fieldConfig);
            if (!config.equals(ForwardIndexConfig.getDefault())) {
              fwdConfig.put(fieldConfig.getName(), config);
            }
            // It is important to do not explicitly add the default value here in order to avoid exclusive problems with
            // the default `fromIndexes` deserializer.
          }
        }
      }
      return fwdConfig;
    };
  }

  private boolean isDisabled(Map<String, String> props) {
    return Boolean.parseBoolean(
        props.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
  }

  private ForwardIndexConfig createConfigFromFieldConfig(FieldConfig fieldConfig) {
    ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
    builder.withCompressionCodec(fieldConfig.getCompressionCodec());
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
      Schema schema, TableConfig tableConfig) {
    return new ForwardIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Override
  protected IndexReaderFactory<ForwardIndexReader> createReaderFactory() {
    return ForwardIndexReaderFactory.getInstance();
  }

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

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    if (columnMetadata == null) {
      return EXTENSIONS;
    }
    return Collections.singletonList(getFileExtension(columnMetadata));
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
        String allocationContext =
            IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (storedType.isFixedWidth() || fixedLengthBytes > 0) {
          return new FixedByteSVMutableForwardIndex(false, storedType, fixedLengthBytes, context.getCapacity(),
              context.getMemoryManager(), allocationContext);
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity =
              Math.min(context.getCapacity(), NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          if (config.getCompressionCodec() == CompressionCodec.CLP) {
            CLPMutableForwardIndexV2 clpMutableForwardIndex =
                new CLPMutableForwardIndexV2(column, context.getMemoryManager());
            // CLP (V1) always have clp encoding enabled whereas V2 is dynamic
            clpMutableForwardIndex.forceClpEncoding();
            return clpMutableForwardIndex;
          } else if (config.getCompressionCodec() == CompressionCodec.CLPV2
              || config.getCompressionCodec() == CompressionCodec.CLPV2_ZSTD
              || config.getCompressionCodec() == CompressionCodec.CLPV2_LZ4) {
            CLPMutableForwardIndexV2 clpMutableForwardIndex =
                new CLPMutableForwardIndexV2(column, context.getMemoryManager());
            return clpMutableForwardIndex;
          }
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
            context.getCapacity(), storedType.size(), context.getMemoryManager(), allocationContext, false, storedType);
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
