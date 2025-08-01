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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
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
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.Intern;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.FALFInterner;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DictionaryIndexType
    extends AbstractIndexType<DictionaryIndexConfig, Dictionary, SegmentDictionaryCreator> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryIndexType.class);
  private static final List<String> EXTENSIONS = Collections.singletonList(V1Constants.Dict.FILE_EXTENSION);

  protected DictionaryIndexType() {
    super(StandardIndexes.DICTIONARY_ID);
  }

  @Override
  public Class<DictionaryIndexConfig> getIndexConfigClass() {
    return DictionaryIndexConfig.class;
  }

  @Override
  public DictionaryIndexConfig getDefaultConfig() {
    return DictionaryIndexConfig.DEFAULT;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    DictionaryIndexConfig dictionaryConfig = indexConfigs.getConfig(StandardIndexes.dictionary());
    if (dictionaryConfig.isEnabled() && dictionaryConfig.getUseVarLengthDictionary()) {
      DataType storedType = fieldSpec.getDataType().getStoredType();
      Preconditions.checkState(!storedType.isFixedWidth(),
          "Cannot create var-length dictionary on column: %s of fixed-width stored type: %s", fieldSpec.getName(),
          storedType);
    }
  }

  @Override
  public String getPrettyName() {
    return getId();
  }

  @Override
  protected ColumnConfigDeserializer<DictionaryIndexConfig> createDeserializerForLegacyConfigs() {
    ColumnConfigDeserializer<DictionaryIndexConfig> fromNoDictionaryConfigs =
        IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getNoDictionaryConfig(),
            (accum, column, value) -> accum.put(column, DictionaryIndexConfig.DISABLED));
    ColumnConfigDeserializer<DictionaryIndexConfig> fromNoDictionaryColumns =
        IndexConfigDeserializer.fromCollection(tableConfig -> tableConfig.getIndexingConfig().getNoDictionaryColumns(),
            (accum, column) -> accum.put(column, DictionaryIndexConfig.DISABLED));
    ColumnConfigDeserializer<DictionaryIndexConfig> fromFieldConfigs =
        IndexConfigDeserializer.fromCollection(TableConfig::getFieldConfigList, (accum, fieldConfig) -> {
          if (fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW) {
            accum.put(fieldConfig.getName(), DictionaryIndexConfig.DISABLED);
          }
        });
    ColumnConfigDeserializer<DictionaryIndexConfig> fromIndexingConfig = (tableConfig, schema) -> {
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      Set<String> onHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns() != null ? new HashSet<>(
          indexingConfig.getOnHeapDictionaryColumns()) : Set.of();
      Set<String> varLengthDictionaryColumns = indexingConfig.getVarLengthDictionaryColumns() != null ? new HashSet<>(
          indexingConfig.getVarLengthDictionaryColumns()) : Set.of();
      Set<String> allColumns = new HashSet<>();
      allColumns.addAll(onHeapDictionaryColumns);
      allColumns.addAll(varLengthDictionaryColumns);
      Map<String, DictionaryIndexConfig> dictionaryIndexConfigMap = Maps.newHashMapWithExpectedSize(allColumns.size());
      for (String column : allColumns) {
        dictionaryIndexConfigMap.put(column, new DictionaryIndexConfig(onHeapDictionaryColumns.contains(column),
            varLengthDictionaryColumns.contains(column)));
      }
      return dictionaryIndexConfigMap;
    };
    return fromNoDictionaryConfigs.withFallbackAlternative(fromNoDictionaryColumns)
        .withFallbackAlternative(fromFieldConfigs)
        .withFallbackAlternative(fromIndexingConfig);
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
    DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    if (storedType != DataType.BYTES && storedType != DataType.BIG_DECIMAL) {
      return false;
    }
    return !context.isFixedLength();
  }

  public static boolean shouldUseVarLengthDictionary(String columnName, Set<String> varLengthDictColumns,
      DataType storedType, ColumnStatistics columnProfile) {
    if (varLengthDictColumns.contains(columnName)) {
      return true;
    }

    return shouldUseVarLengthDictionary(storedType, columnProfile);
  }

  public static boolean shouldUseVarLengthDictionary(DataType storedType, ColumnStatistics profile) {
    if (storedType == DataType.BYTES || storedType == DataType.BIG_DECIMAL) {
      return !profile.isFixedLength();
    }

    return false;
  }

  /**
   * Similar to shouldUseVarLengthDictionary, but also checks STRING type. Separated due to backwards compatibility
   * concerns.
   */
  public static boolean optimizeTypeShouldUseVarLengthDictionary(DataType storedType, ColumnStatistics profile) {
    if (storedType == DataType.BYTES || storedType == DataType.BIG_DECIMAL || storedType == DataType.STRING) {
      return !profile.isFixedLength();
    }

    return false;
  }


  /**
   * This function evaluates whether to override dictionary (i.e use noDictionary)
   * for a column even when its explicitly configured. This evaluation is for both dimension and metric
   * column types.
   *
   * @return true if dictionary should be created, false if noDictionary should be used
   */
  public static boolean ignoreDictionaryOverride(boolean optimizeDictionary, boolean optimizeDictionaryForMetrics,
      double noDictionarySizeRatioThreshold, @Nullable Double noDictionaryCardinalityRatioThreshold,
      FieldSpec fieldSpec, FieldIndexConfigs fieldIndexConfigs, int cardinality, int totalNumberOfEntries) {
    // For an inverted index dictionary is required
    if (fieldIndexConfigs.getConfig(StandardIndexes.inverted()).isEnabled()) {
      return true;
    }
    if (optimizeDictionary) {
      // Do not create dictionaries for json or text index columns as they are high-cardinality values almost always
      if ((fieldIndexConfigs.getConfig(StandardIndexes.json()).isEnabled() || fieldIndexConfigs.getConfig(
          StandardIndexes.text()).isEnabled())) {
        return false;
      }
      // Do not create dictionary if index size with dictionary is going to be larger than index size without dictionary
      // This is done to reduce the cost of dictionary for high cardinality columns
      // Off by default and needs optimizeDictionary to be set to true
      if (fieldSpec.isSingleValueField()) {
        return ignoreDictionaryOverrideForSingleValueFields(cardinality, totalNumberOfEntries,
            noDictionarySizeRatioThreshold, noDictionaryCardinalityRatioThreshold, fieldSpec);
      }
    }
    if (optimizeDictionaryForMetrics && !optimizeDictionary && fieldSpec.isSingleValueField()
        && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC) {
      return ignoreDictionaryOverrideForSingleValueFields(cardinality, totalNumberOfEntries,
          noDictionarySizeRatioThreshold, noDictionaryCardinalityRatioThreshold, fieldSpec);
    }
    return true;
  }

  /**
   * Hold common logic for ignoring dictionary override for single value fields, used for dim and metric cols
   */
  private static boolean ignoreDictionaryOverrideForSingleValueFields(int cardinality, int totalNumberOfEntries,
      double noDictionarySizeRatioThreshold, Double noDictionaryCardinalityRatioThreshold, FieldSpec fieldSpec) {
    if (fieldSpec.isSingleValueField()) {
      if (fieldSpec.getDataType().isFixedWidth()) {
        // if you can safely enable dictionary, you can ignore overrides
        return canSafelyCreateDictionaryWithinThreshold(cardinality, totalNumberOfEntries,
            noDictionarySizeRatioThreshold, fieldSpec);
      }
      // Config not set, default to old behavior of create dictionary for var width cols
      if (noDictionaryCardinalityRatioThreshold == null) {
        return true;
      }
      // Variable width type, so create based simply on cardinality threshold since size cannot be calculated easily
      return noDictionaryCardinalityRatioThreshold * totalNumberOfEntries > cardinality;
    }
    return false;
  }

  /**
   * Given the column cardinality, totalNumberOfEntries, this function checks if the savings ratio
   * is larger than the configured threshold (noDictionarySizeRatioThreshold). If savings ratio is
   * smaller than the threshold, we want to override to noDictionary.
   */
  private static boolean canSafelyCreateDictionaryWithinThreshold(int cardinality, int totalNumberOfEntries,
      double noDictionarySizeRatioThreshold, FieldSpec spec) {
    long dictionarySize = cardinality * (long) spec.getDataType().size();
    long forwardIndexSize =
        ((long) totalNumberOfEntries * PinotDataBitSet.getNumBitsPerValue(cardinality - 1)
            + Byte.SIZE - 1) / Byte.SIZE;

    double indexWithDictSize = dictionarySize + forwardIndexSize;
    double indexWithoutDictSize = totalNumberOfEntries * spec.getDataType().size();

    double indexSizeRatio = indexWithoutDictSize / indexWithDictSize;
    if (indexSizeRatio <= noDictionarySizeRatioThreshold) {
      return false;
    }
    return true;
  }

  public SegmentDictionaryCreator createIndexCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary)
      throws Exception {
    return new SegmentDictionaryCreator(fieldSpec, indexDir, useVarLengthDictionary);
  }

  public static Dictionary read(SegmentDirectory.Reader segmentReader, ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dataBuffer =
        segmentReader.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.dictionary());
    return read(dataBuffer, columnMetadata, DictionaryIndexConfig.DEFAULT);
  }

  public static Dictionary read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, DictionaryIndexConfig indexConfig)
      throws IOException {
    return read(dataBuffer, metadata, indexConfig, null);
  }

  public static Dictionary read(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
      DictionaryIndexConfig indexConfig, String internIdentifierStr)
      throws IOException {

    DataType dataType = metadata.getDataType();
    boolean loadOnHeap = indexConfig.isOnHeap();
    String columnName = metadata.getColumnName();

    // If interning is enabled, get the required interners.
    FALFInterner<String> strInterner = null;
    FALFInterner<byte[]> byteInterner = null;
    Intern internConfig = indexConfig.getIntern();
    if (loadOnHeap) {
      LOGGER.info("Loading on-heap dictionary for column: {}", columnName);
      if (internConfig != null && !internConfig.isDisabled()) {
        DictionaryInternerHolder internerHolder = DictionaryInternerHolder.getInstance();
        strInterner = internerHolder.getStrInterner(internIdentifierStr, internConfig.getCapacity());
        byteInterner = internerHolder.getByteInterner(internIdentifierStr, internConfig.getCapacity());
        LOGGER.info("Enabling interning for dictionary column: {}", columnName);
      }
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
        return loadOnHeap ? new OnHeapStringDictionary(dataBuffer, length, numBytesPerValue, strInterner, byteInterner)
            : new StringDictionary(dataBuffer, length, numBytesPerValue);
      case BYTES:
        numBytesPerValue = metadata.getColumnMaxLength();
        return loadOnHeap ? new OnHeapBytesDictionary(dataBuffer, length, numBytesPerValue, byteInterner)
            : new BytesDictionary(dataBuffer, length, numBytesPerValue);
      default:
        throw new IllegalStateException("Unsupported data type for dictionary: " + dataType);
    }
  }

  @Override
  protected IndexReaderFactory<Dictionary> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return IndexHandler.NoOp.INSTANCE;
  }

  public static String getFileExtension() {
    return V1Constants.Dict.FILE_EXTENSION;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<DictionaryIndexConfig, Dictionary> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }
    @Override
    protected IndexType<DictionaryIndexConfig, Dictionary, ?> getIndexType() {
      return StandardIndexes.dictionary();
    }

    @Override
    protected Dictionary createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        DictionaryIndexConfig indexConfig)
        throws IOException, IndexReaderConstraintException {
      return DictionaryIndexType.read(dataBuffer, metadata, indexConfig);
    }

    @Override
    public Dictionary createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
        ColumnMetadata metadata) throws IOException, IndexReaderConstraintException {
      String colName = metadata.getColumnName();

      if (!segmentReader.hasIndexFor(colName, StandardIndexes.dictionary())) {
        return null;
      }

      PinotDataBuffer buffer = segmentReader.getIndexFor(colName, StandardIndexes.dictionary());
      DictionaryIndexConfig config = fieldIndexConfigs.getConfig(StandardIndexes.dictionary());
      String tableName = segmentReader.toSegmentDirectory().getSegmentMetadata().getTableName();
      String internIdentifierStr = DictionaryInternerHolder.getInstance().createIdentifier(tableName, colName);

      try {
        return DictionaryIndexType.read(buffer, metadata, config, internIdentifierStr);
      } catch (RuntimeException ex) {
        throw new RuntimeException("Cannot read index " + StandardIndexes.dictionary() + " for column " + colName, ex);
      }
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns() == null
        ? Lists.newArrayList()
        : indexingConfig.getNoDictionaryColumns();
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList() == null
        ? Lists.newArrayList()
        : tableConfig.getFieldConfigList();

    List<FieldConfig> configsToUpdate = new ArrayList<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      // skip further computation of field configs which already has RAW encodingType
      if (fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW) {
        continue;
      }
      // ensure encodingType is RAW on noDictionaryColumns
      if (noDictionaryColumns.remove(fieldConfig.getName())) {
        configsToUpdate.add(fieldConfig);
      }
      if (fieldConfig.getIndexes() == null || fieldConfig.getIndexes().get(getPrettyName()) == null) {
        continue;
      }
      try {
        DictionaryIndexConfig indexConfig = JsonUtils.jsonNodeToObject(
            fieldConfig.getIndexes().get(getPrettyName()),
            DictionaryIndexConfig.class);
        // ensure encodingType is RAW where dictionary index config has disabled = true
        if (indexConfig.isDisabled()) {
          configsToUpdate.add(fieldConfig);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    // update the encodingType to RAW on the selected field configs
    for (FieldConfig fieldConfig : configsToUpdate) {
      FieldConfig.Builder builder = new FieldConfig.Builder(fieldConfig);
      builder.withEncodingType(FieldConfig.EncodingType.RAW);
      fieldConfigList.remove(fieldConfig);
      fieldConfigList.add(builder.build());
    }

    // create the missing field config for the remaining noDictionaryColumns
    for (String column : noDictionaryColumns) {
      FieldConfig.Builder builder = new FieldConfig.Builder(column);
      builder.withEncodingType(FieldConfig.EncodingType.RAW);
      fieldConfigList.add(builder.build());
    }

    // old configs cleanup
    indexingConfig.setNoDictionaryConfig(null);
    indexingConfig.setNoDictionaryColumns(null);
    indexingConfig.setOnHeapDictionaryColumns(null);
    indexingConfig.setVarLengthDictionaryColumns(null);
  }

  /**
   * Creates a MutableDictionary.
   *
   * Unlikes most indexes, while dictionaries are important when
   * {@link org.apache.pinot.segment.spi.MutableSegment mutable segments} are created, they do not follow the
   * {@link MutableIndex} interface and therefore
   * {@link DictionaryIndexType#createMutableIndex(MutableIndexContext, IndexConfig)} is not implemented.
   *
   * This also means that dictionaries cannot be overridden in realtime tables.
   */
  @Nullable
  public static MutableDictionary createMutableDictionary(MutableIndexContext context, DictionaryIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    String column = context.getFieldSpec().getName();
    String segmentName = context.getSegmentName();
    DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    int dictionaryColumnSize;
    if (storedType.isFixedWidth()) {
      dictionaryColumnSize = storedType.size();
    } else {
      dictionaryColumnSize = context.getEstimatedColSize();
    }
    // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
    // TODO(mutable-index-spi): Actually this 10% extra was applied twice, multiplying the cardinality by 1.21
    //  first time it was applied in MutableSegmentImpl and then in DefaultMutableIndexProvider (where this code was
    //  copied from). Decide if we want to actually use 10% as the comment said or 21% as we were doing
    int estimatedCardinality = (int) (context.getEstimatedCardinality() * 1.21);
    String dictionaryAllocationContext =
        IndexUtil.buildAllocationContext(segmentName, column, V1Constants.Dict.FILE_EXTENSION);
    return MutableDictionaryFactory.getMutableDictionary(storedType, context.isOffHeap(), context.getMemoryManager(),
        dictionaryColumnSize, Math.min(estimatedCardinality, context.getCapacity()), dictionaryAllocationContext);
  }

  public BuildLifecycle getIndexBuildLifecycle() {
    return BuildLifecycle.CUSTOM;
  }
}
