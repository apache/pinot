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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ForwardIndexType extends AbstractIndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexType.class);

  public static final String INDEX_DISPLAY_NAME = "forward";
  public static final String ENCODING_TYPE_CONFIG_KEY = "encodingType";
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
    return ForwardIndexConfig.getDefault(FieldConfig.EncodingType.DICTIONARY);
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
    // Dictionary-encoded forward index requires a dictionary to translate dict ids back to values.
    if (forwardIndexConfig.getEncodingType() == FieldConfig.EncodingType.DICTIONARY) {
      Preconditions.checkState(dictionaryConfig.isEnabled(),
          "Dictionary must be enabled for dictionary-encoded forward index column: %s", column);
    }
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

  /// Resolves per-column [ForwardIndexConfig] in a single pass over [TableConfig], reconciling the legacy signals
  /// (`indexingConfig.noDictionaryColumns`, `indexingConfig.noDictionaryConfig`, deprecated
  /// `fieldConfig.encodingType`, `fieldConfig.compressionCodec`) with the modern `fieldConfig.indexes.forward` JSON
  /// block. When `indexes.forward.encodingType` is present, it is the canonical forward-index encoding and wins over
  /// legacy top-level signals.
  @Override
  @SuppressWarnings("deprecation")
  protected ColumnConfigDeserializer<ForwardIndexConfig> createDeserializer() {
    return (tableConfig, schema) -> {
      Map<String, ForwardIndexConfig> result = new HashMap<>();

      // Legacy noDictionary signals — both indicate RAW forward index for the listed columns.
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      Set<String> noDictionaryColumns = new HashSet<>();
      if (indexingConfig.getNoDictionaryColumns() != null) {
        noDictionaryColumns.addAll(indexingConfig.getNoDictionaryColumns());
      }
      Map<String, String> noDictionaryConfig = indexingConfig.getNoDictionaryConfig();
      if (noDictionaryConfig != null) {
        noDictionaryColumns.addAll(noDictionaryConfig.keySet());
      }

      Collection<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          String column = fieldConfig.getName();
          // Pop processed columns so the post-loop scan only emits defaults for columns with no FieldConfig at all.
          boolean inNoDictionaryList = noDictionaryColumns.remove(column);

          // `forwardIndexDisabled` short-circuits everything else.
          Map<String, String> properties = fieldConfig.getProperties();
          if (properties != null && isDisabled(properties)) {
            result.put(column, ForwardIndexConfig.getDisabled());
            continue;
          }

          FieldConfig.CompressionCodec fcCodec = fieldConfig.getCompressionCodec();
          FieldConfig.EncodingType legacyEncodingType =
              inNoDictionaryList ? FieldConfig.EncodingType.RAW : fieldConfig.getEncodingType();

          JsonNode forwardIndexNode = fieldConfig.getIndexes().get(INDEX_DISPLAY_NAME);
          if (forwardIndexNode != null) {
            Preconditions.checkState(forwardIndexNode.isObject(), "Invalid forward index config for column: %s",
                column);

            JsonNode innerEncodingNode = forwardIndexNode.get(ENCODING_TYPE_CONFIG_KEY);

            // Conflict: compressionCodec mismatch between FieldConfig and indexes.forward.
            JsonNode innerCodecNode = forwardIndexNode.get("compressionCodec");
            if (innerCodecNode != null && !innerCodecNode.isNull() && fcCodec != null) {
              FieldConfig.CompressionCodec inner = FieldConfig.CompressionCodec.valueOf(innerCodecNode.asText());
              Preconditions.checkState(inner == fcCodec,
                  "Conflicting forward-index compressionCodec for column: %s — FieldConfig.compressionCodec=%s "
                      + "but indexes.forward.compressionCodec=%s", column, fcCodec, inner);
            }

            // Inject legacy encodingType / compressionCodec into the JSON only when the forward block omits them.
            ObjectNode configNode = (ObjectNode) forwardIndexNode.deepCopy();
            if (innerEncodingNode == null || innerEncodingNode.isNull()) {
              configNode.put(ENCODING_TYPE_CONFIG_KEY, legacyEncodingType.name());
            }
            if ((innerCodecNode == null || innerCodecNode.isNull()) && fcCodec != null) {
              configNode.put("compressionCodec", fcCodec.name());
            }
            try {
              result.put(column, JsonUtils.jsonNodeToObject(configNode, ForwardIndexConfig.class));
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          } else {
            result.put(column, createConfigFromFieldConfig(fieldConfig, legacyEncodingType));
          }
        }
      }

      // Columns listed in noDictionaryColumns/noDictionaryConfig that have no FieldConfig at all — emit a RAW default.
      // (Columns with a FieldConfig were already removed from `noDictionaryColumns` above.)
      for (String column : noDictionaryColumns) {
        result.put(column, ForwardIndexConfig.getDefault(FieldConfig.EncodingType.RAW));
      }
      return result;
    };
  }

  private boolean isDisabled(Map<String, String> props) {
    return Boolean.parseBoolean(
        props.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
  }

  private ForwardIndexConfig createConfigFromFieldConfig(FieldConfig fieldConfig,
      FieldConfig.EncodingType resolvedEncodingType) {
    ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder(resolvedEncodingType)
        .withCompressionCodec(fieldConfig.getCompressionCodec());
    Map<String, String> properties = fieldConfig.getProperties();
    if (properties != null) {
      builder.withLegacyProperties(properties);
    }
    return builder.build();
  }

  /// Returns the canonical forward-index encoding for `fieldConfig`.
  ///
  /// Prefer `fieldConfig.indexes.forward.encodingType` when present. Fall back to the deprecated field-level
  /// `fieldConfig.encodingType` for legacy conversion paths that can still inspect configs before validation.
  @SuppressWarnings("deprecation")
  public static FieldConfig.EncodingType getForwardEncodingType(FieldConfig fieldConfig) {
    JsonNode indexes = fieldConfig.getIndexes();
    if (indexes != null && indexes.isObject()) {
      JsonNode forward = indexes.get(INDEX_DISPLAY_NAME);
      if (forward != null && forward.isObject()) {
        JsonNode encodingType = forward.get(ENCODING_TYPE_CONFIG_KEY);
        if (encodingType != null && !encodingType.isNull()) {
          return FieldConfig.EncodingType.valueOf(encodingType.asText());
        }
      }
    }
    return fieldConfig.getEncodingType();
  }

  /// Returns a copy of `indexes` with `indexes.forward.encodingType` set to `encodingType`.
  public static JsonNode withForwardEncoding(@Nullable JsonNode indexes, FieldConfig.EncodingType encodingType) {
    ObjectNode indexesNode = indexes != null && indexes.isObject()
        ? (ObjectNode) indexes.deepCopy()
        : JsonUtils.newObjectNode();
    JsonNode forward = indexesNode.get(INDEX_DISPLAY_NAME);
    ObjectNode forwardNode = forward != null && forward.isObject()
        ? (ObjectNode) forward.deepCopy()
        : JsonUtils.newObjectNode();
    forwardNode.put(ENCODING_TYPE_CONFIG_KEY, encodingType.name());
    indexesNode.set(INDEX_DISPLAY_NAME, forwardNode);
    return indexesNode;
  }

  public static ChunkCompressionType getDefaultCompressionType(FieldSpec.FieldType fieldType) {
    if (fieldType == FieldSpec.FieldType.METRIC) {
      return ChunkCompressionType.PASS_THROUGH;
    } else {
      return ChunkCompressionType.LZ4;
    }
  }

  @Override
  public boolean shouldCreateIndex(IndexCreationContext context, ForwardIndexConfig indexConfig) {
    return context.getFieldSpec().getDataType() != FieldSpec.DataType.OPEN_STRUCT;
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
  public boolean requiresDictionary(FieldSpec fieldSpec, ForwardIndexConfig indexConfig) {
    // Forward index supports both DICT and RAW encodings; it does not require a dictionary to function.
    return false;
  }

  @Override
  public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, ForwardIndexConfig indexConfig) {
    // The forward index encoding is reconciled with ForwardIndexConfig independently of dictionary state
    // (apache/pinot#18364), so adding or removing a dictionary alone does not change the on-disk forward index
    // layout — the existing forward index can be reused.
    return false;
  }

  @Override
  protected IndexReaderFactory<ForwardIndexReader> createReaderFactory() {
    return ForwardIndexReaderFactory.getInstance();
  }

  public String getFileExtension(ColumnMetadata columnMetadata) {
    boolean isRaw = columnMetadata.getForwardIndexEncoding() == FieldConfig.EncodingType.RAW;
    if (columnMetadata.isSingleValue()) {
      if (isRaw) {
        return V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else if (columnMetadata.isSorted()) {
        return V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        return V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else if (isRaw) {
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
