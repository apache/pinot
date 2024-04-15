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
package org.apache.pinot.segment.local.segment.creator.impl.map;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnStatistics;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.MapColumnStatistics;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_FORWARD_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Creates the durable representation of a map index. Metadata about the Map Column can be passed through via
 * the IndexCreationContext.
 */
public final class MapIndexCreator implements org.apache.pinot.segment.spi.index.creator.MapIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapIndexCreator.class);
  //output file which will hold the range index
  private final String _tmpMapIndexDir;
  private final String _indexDir;
  private final Map<String, Map<IndexType<?, ?, ?>, IndexCreator>> _creatorsByKeyAndIndex;
  private final TreeMap<String, ColumnIndexCreationInfo> _keyIndexCreationInfoMap = new TreeMap<>();
  private final Map<String, FieldSpec> _denseKeySpecs;
  private final Set<String> _denseKeys;
  private final int _totalDocs;
  private final Map<String, ColumnStatistics> _keyStats = new HashMap<>();
  private final Map<String, ColumnMetadata> _denseKeyMetadata = new HashMap<>();
  private final String _columnName;
  private int _docsWritten;
  private final IndexCreationContext _context;
  private final MapColumnStatistics _mapColumnStatistics;
  private final boolean _dynamicallyCreateDenseKeys;

  /**
   *
   * @param context The Index Creation Context, used for configuring many of the options for index creation.
   * @param columnName name of the column
   * @throws IOException
   */
  public MapIndexCreator(IndexCreationContext context, String columnName, MapIndexConfig config)
      throws IOException {
    // The Dense map column is composed of other indexes, so we'll store those index in a subdirectory
    // Then when those indexes are created, they are created in this column's subdirectory.
    _indexDir = context.getIndexDir().getPath();
    _mapColumnStatistics = (MapColumnStatistics) context.getColumnStatistics();
    _tmpMapIndexDir = String.format("%s/tmp_%s/", _indexDir, columnName + MAP_FORWARD_INDEX_FILE_EXTENSION);
    final File denseKeyDir = new File(_tmpMapIndexDir);
    try {
      if (!denseKeyDir.mkdirs()) {
        LOGGER.error("Failed to create directory: {}", denseKeyDir);
      }
    } catch (Exception ex) {
      LOGGER.error("Exception while creating temporary directory: '{}'", denseKeyDir, ex);
    }

    config = config != null ? config : new MapIndexConfig();
    _context = context;
    _totalDocs = context.getTotalDocs();
    _denseKeySpecs = new HashMap<>(config.getMaxKeys());
    _dynamicallyCreateDenseKeys = config.getDynamicallyCreateDenseKeys();
    _denseKeys = new HashSet<>();
    _columnName = columnName;
    _docsWritten = 0;
    _creatorsByKeyAndIndex = Maps.newHashMapWithExpectedSize(_denseKeySpecs.size());

    if (config.getDenseKeys() != null) {
      // If the user has pre-configured as certain keys then start by creating the writers for those keys
      for (DimensionFieldSpec key : config.getDenseKeys()) {
        addKeyWriter(key.getName(), key.getDataType().getStoredType());
      }
    }

    if (_dynamicallyCreateDenseKeys) {
      // If the user has enabled dynamic key creation, then iterate through the map column statistics to create
      // key writers
      if (_mapColumnStatistics != null && _mapColumnStatistics.getKeys() != null) {
        for (Pair<String, DataType> key : _mapColumnStatistics.getKeys()) {
          addKeyWriter(key.getLeft(), key.getRight());
        }
      }
    }
  }

  private void addKeyWriter(String keyName, DataType type) {
    FieldSpec keySpec = createKeyFieldSpec(keyName, type);
    _denseKeySpecs.put(keyName, keySpec);
    _denseKeys.add(keySpec.getName());

    ColumnStatistics stats = buildKeyStats(keyName);
    _keyStats.put(keyName, stats);

    ColumnMetadata keyMetadata = generateColumnMetadataForKey(keySpec);
    _denseKeyMetadata.put(keyName, keyMetadata);

    ColumnIndexCreationInfo creationInfo = buildIndexCreationInfoForKey(keySpec);
    _keyIndexCreationInfoMap.put(keyName, creationInfo);

    Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex = createIndexCreatorsForKey(keySpec);
    _creatorsByKeyAndIndex.put(keySpec.getName(), creatorsByIndex);
  }

  private FieldSpec createKeyFieldSpec(String name, DataType type) {
    FieldSpec keySpec = new DimensionFieldSpec();
    keySpec.setName(name);
    keySpec.setDataType(type);
    keySpec.setNullable(true);
    keySpec.setSingleValueField(true);
    keySpec.setDefaultNullValue(null);  // Sets the default default null value

    return keySpec;
  }

  private ColumnStatistics buildKeyStats(String key) {
    Object minValue = _mapColumnStatistics.getMinValueForKey(key);
    Object maxValue = _mapColumnStatistics.getMaxValueForKey(key);

    // The length of the shortest and longest should be at least the length of the null value
    int lengthShortest = Math.max(_mapColumnStatistics.getLengthOfShortestElementForKey(key), 4);
    int lengthLongest = Math.max(_mapColumnStatistics.getLengthOfLargestElementForKey(key), 4);

    return new DefaultColumnStatistics(minValue, maxValue, null,
        false, _totalDocs, 0, lengthShortest, lengthLongest);
  }

  private Map<IndexType<?, ?, ?>, IndexCreator> createIndexCreatorsForKey(FieldSpec keySpec) {
    boolean dictEnabledColumn =
        false; //createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);
    ColumnIndexCreationInfo columnIndexCreationInfo = _keyIndexCreationInfoMap.get(keySpec.getName());

    FieldIndexConfigs keyConfig = getKeyIndexConfig(columnIndexCreationInfo);
    IndexCreationContext.Common keyContext =
        getKeyIndexContext(dictEnabledColumn, keySpec, columnIndexCreationInfo, _context);
    Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex =
        Maps.newHashMapWithExpectedSize(IndexService.getInstance().getAllIndexes().size());
    for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
      if (index.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
        continue;
      }

      try {
        tryCreateIndexCreator(creatorsByIndex, index, keyContext, keyConfig);
      } catch (Exception e) {
        LOGGER.error("An exception happened while creating IndexCreator for key '{}' for index '{}'", keySpec.getName(),
            index.getId(), e);
      }
    }

    return creatorsByIndex;
  }

  private FieldIndexConfigs getKeyIndexConfig(ColumnIndexCreationInfo columnIndexCreationInfo) {
    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
    // Sorted columns treat the 'forwardIndexDisabled' flag as a no-op
    // ForwardIndexConfig fwdConfig = config.getConfig(StandardIndexes.forward());

    ForwardIndexConfig fwdConfig = new ForwardIndexConfig.Builder()
        // TODO (make configurable): .withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH)
        //.withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH)
        .build();
    // TODO(What's this for?)   if (!fwdConfig.isEnabled() && columnIndexCreationInfo.isSorted()) {
    builder.add(StandardIndexes.forward(), new ForwardIndexConfig.Builder(fwdConfig).build());
    //}
    // Initialize inverted index creator; skip creating inverted index if sorted
    if (columnIndexCreationInfo.isSorted()) {
      builder.undeclare(StandardIndexes.inverted());
    }
    return builder.build();
  }

  private IndexCreationContext.Common getKeyIndexContext(boolean dictEnabledColumn, FieldSpec keySpec,
      ColumnIndexCreationInfo columnIndexCreationInfo, IndexCreationContext context) {
    File denseKeyDir = new File(_tmpMapIndexDir);
    return IndexCreationContext.builder()
        .withIndexDir(denseKeyDir)
        .withDictionary(dictEnabledColumn)
        .withFieldSpec(keySpec)
        .withTotalDocs(_totalDocs)
        .withColumnIndexCreationInfo(columnIndexCreationInfo)
        .withMaxRowLengthInBytes(_keyStats.get(keySpec.getName()).getMaxRowLengthInBytes())
        .withLengthOfLongestEntry(_keyStats.get(keySpec.getName()).getLengthOfLargestElement())
        .withMinValue((Comparable) _keyStats.get(keySpec.getName()).getMinValue())
        .withMaxValue((Comparable) _keyStats.get(keySpec.getName()).getMaxValue())
        .withOptimizedDictionary(false)
        .onHeap(context.isOnHeap())
        .withForwardIndexDisabled(false)
        .withTextCommitOnClose(true)
        .build();
  }

  ColumnIndexCreationInfo buildIndexCreationInfoForKey(FieldSpec keySpec) {
    // TODO: Does this need to do anything with variable length columns?  Search for varLengthDictionaryColumns
    String keyName = keySpec.getName();
    DataType storedType = keySpec.getDataType().getStoredType();
    ColumnStatistics columnProfile = null;
    try {
      columnProfile = _keyStats.get(keyName);
    } catch (Exception ex) {
      LOGGER.error("Failed to get profile for key: '{}'", keyName, ex);
    }
    boolean useVarLengthDictionary = false;
    //shouldUseVarLengthDictionary(columnName, varLengthDictionaryColumns, storedType, columnProfile);
    Object defaultNullValue = keySpec.getDefaultNullValue();
    if (storedType == DataType.BYTES) {
      defaultNullValue = new ByteArray((byte[]) defaultNullValue);
    }
    boolean createDictionary = false;
    //!rawIndexCreationColumns.contains(keyName) && !rawIndexCompressionTypeKeys.contains(keyName);
   return new ColumnIndexCreationInfo(columnProfile, createDictionary, useVarLengthDictionary, false/*isAutoGenerated*/,
            defaultNullValue);
  }

  private <C extends IndexConfig> void tryCreateIndexCreator(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      IndexType<C, ?, ?> index, IndexCreationContext.Common context, FieldIndexConfigs fieldIndexConfigs)
      throws Exception {
    C config = fieldIndexConfigs.getConfig(index);
    if (config.isEnabled()) {
      creatorsByIndex.put(index, index.createIndexCreator(context, config));
    }
  }

  @Override
  public void seal()
      throws IOException {
    for (Map.Entry<String, Map<IndexType<?, ?, ?>, IndexCreator>> keysInMap : _creatorsByKeyAndIndex.entrySet()) {
      for (IndexCreator keyIdxCreator : keysInMap.getValue().values()) {
        keyIdxCreator.close();
        keyIdxCreator.seal();
      }
    }

    // Stitch the index files together
    mergeKeyFiles();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return DataType.MAP;
  }

  private void mergeKeyFiles() {
    File mergedIndexFile = new File(_indexDir, _columnName + MAP_FORWARD_INDEX_FILE_EXTENSION);

    try {
      // Construct the Header for the merged index
      long totalIndexLength = 0;

      // Compute the total size of the indexes
      HashMap<String, Long> indexSizes = new HashMap<>();
      Map<String, File> keyFiles = new HashMap<>(_denseKeys.size());
      for (String key : _denseKeys) {
        File keyFile = getFileFor(key, StandardIndexes.forward());
        final long indexSize = Files.size(keyFile.toPath());
        indexSizes.put(key, indexSize);
        totalIndexLength += indexSize;
        keyFiles.put(key, keyFile);
        _denseKeyMetadata.get(key).getIndexSizeMap().put(StandardIndexes.forward(), indexSize);
      }

      // Construct the Header
      MapIndexHeader header = new MapIndexHeader();
      DenseMapHeader denseHeader = new DenseMapHeader();
      for (String key : _denseKeys) {
        // Write with a placeholder offset, it will be updated when the indexes are merged
        //header.addKey(key, _denseKeyMetadata.get(key), 0xDEADBEEF);
        denseHeader.addKey(key, _denseKeyMetadata.get(key), List.of(StandardIndexes.forward()), 0);
      }
      header.addMapIndex(denseHeader);

      // Create an output buffer for writing to (see the V2 to V3 conversion logic for what to do here)
      final long totalFileSize = header.size() + totalIndexLength;
      long offset = 0;
      PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(mergedIndexFile, false, offset, totalFileSize, ByteOrder.BIG_ENDIAN,
              allocationContext(mergedIndexFile, "writing"));

      // Write the header to the buffer
      MapIndexHeader.PinotDataBufferWriter writer = new MapIndexHeader.PinotDataBufferWriter(buffer);
      offset = header.write(writer, offset);

      // Iterate over each key and find the index and write the index to a file
      for (String key : _denseKeys) {
        File keyFile = keyFiles.get(key);
        try (FileChannel denseKeyFileChannel = new RandomAccessFile(keyFile, "r").getChannel()) {
          long indexSize = denseKeyFileChannel.size();
          try (PinotDataBuffer keyBuffer = PinotDataBuffer.mapFile(keyFile, true, 0, indexSize, ByteOrder.BIG_ENDIAN,
              allocationContext(keyFile, "reading"))) {
            keyBuffer.copyTo(0, buffer, offset, indexSize);
            denseHeader.getKey(key).setIndexOffset(writer, StandardIndexes.forward(), offset);
            offset += indexSize;
          } catch (Exception ex) {
            LOGGER.error("Error mapping PinotDataBuffer for '{}'", keyFile, ex);
          }
        } catch (Exception ex) {
          LOGGER.error("Error opening dense key file '{}': ", keyFile, ex);
        }
      }

      // Delete the index files
      buffer.close();

      deleteIntermediateFiles(keyFiles);
    } catch (Exception ex) {
      LOGGER.error("Exception while merging dense key indexes: ", ex);
    }
  }

  private void deleteIntermediateFiles(Map<String, File> files) {
    for (File file : files.values()) {
      try {
        if (!file.delete()) {
          LOGGER.error("Failed to delete file '{}'. Reason is unknown", file);
        }
      } catch (Exception ex) {
        LOGGER.error("Failed to delete intermediate file '{}'", file, ex);
      }
    }

    File tmpDir = new File(_tmpMapIndexDir);
    try {
      // Delete the temporary directory
      if (!tmpDir.delete()) {
        LOGGER.error("Failed to delete directory '{}'", tmpDir);
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to delete temporary directory: '{}'", tmpDir, ex);
    }
  }

  private ColumnMetadata generateColumnMetadataForKey(FieldSpec keySpec) {
    ColumnMetadataImpl.Builder builder = new ColumnMetadataImpl.Builder();
    HashMap<IndexType<?, ?, ?>, Long> indexSizeMap = new HashMap<>();

    Comparable<?> minValue = getPlaceholderValue(keySpec.getDataType());
    Comparable<?> maxValue = getPlaceholderValue(keySpec.getDataType());
    builder
        .setFieldSpec(keySpec)
        .setMinValue(minValue)
        .setMaxValue(maxValue)
        .setMinMaxValueInvalid(true)
        .setCardinality(0)
        .setSorted(false)
        .setHasDictionary(false)
        .setColumnMaxLength(10)
        .setBitsPerElement(10)
        .setAutoGenerated(false)
        .setTotalDocs(10);
    builder.setIndexSizeMap(indexSizeMap);
    return builder.build();
  }

  private void backFillKey(String key, Object value) throws IOException {
    for (int i = 0; i < _docsWritten; i++) {
      try {
        // Iterate over each key in the dictionary and if it exists in the record write a value, otherwise write
        // the null value
        for (Map.Entry<IndexType<?, ?, ?>, IndexCreator> indexes : _creatorsByKeyAndIndex.get(key).entrySet()) {
          indexes.getValue().add(value, -1); // TODO: Add in dictionary encoding support
        }
      } catch (IOException ioe) {
        LOGGER.error("Error writing to dense key '{}': ", key, ioe);
        throw ioe;
      } catch (Exception e) {
        LOGGER.error("Error getting dense key '{}': ", key, e);
      }
    }
  }

  private void dynamicallyAddKeys(Map<String, Object> mapValue) {
    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
      // Check if the key exists already
      String keyName = entry.getKey();
      if (!_keyIndexCreationInfoMap.containsKey(keyName)) {
        // If the key does not exist then create a writer for it
        DataType valType = convertToDataType(PinotDataType.getSingleValueType(entry.getValue().getClass()));
        addKeyWriter(keyName, valType);

        // Backfill the index
        try {
          backFillKey(keyName, _denseKeySpecs.get(keyName).getDefaultNullValue());
        } catch (Exception ex) {
          LOGGER.error("Failed to write to key '{}'", keyName, ex);
        }
      }
    }
  }

  @Override
  public void add(Map<String, Object> mapValue) {
    if (_dynamicallyCreateDenseKeys) {
      dynamicallyAddKeys(mapValue);
    }

    // Iterate over every dense key in this map
    for (String key : _denseKeys) {
      FieldSpec denseKey = _denseKeySpecs.get(key);
      String keyName = denseKey.getName();

      // Get the value of the key from the input map and write to each index
      Object value = mapValue.get(keyName);

      // If the value is NULL or the value's type does not match the key's index type then
      // Write the default value to the index
      if (value == null) {
        value = _keyIndexCreationInfoMap.get(keyName).getDefaultNullValue();
      } else {
        DataType valType = convertToDataType(PinotDataType.getSingleValueType(value.getClass()));
        if (!valType.equals(denseKey.getDataType())) {
          LOGGER.warn("Type mismatch, expected '{}' but got '{}'", denseKey.getDataType(), valType);
          value = _keyIndexCreationInfoMap.get(keyName).getDefaultNullValue();
        }
      }

      // Get the type of the value to check that it matches the Dense Key's type
      try {
        // Iterate over each key in the dictionary and if it exists in the record write a value, otherwise write
        // the null value
        for (Map.Entry<IndexType<?, ?, ?>, IndexCreator> indexes : _creatorsByKeyAndIndex.get(keyName).entrySet()) {
          indexes.getValue().add(value, -1); // TODO: Add in dictionary encoding support
        }
      } catch (IOException ioe) {
        LOGGER.error("Error writing to dense key '{}': ", keyName, ioe);
      } catch (Exception e) {
        LOGGER.error("Error getting dense key '{}': ", keyName, e);
      }
    }

    _docsWritten++;
  }

  public void close()
      throws IOException {
  }

  File getFileFor(String column, IndexType<?, ?, ?> indexType) {
    List<File> candidates = getFilesFor(column, indexType);
    if (candidates.isEmpty()) {
      throw new RuntimeException("No file candidates for index " + indexType + " and column " + column);
    }

    return candidates.stream().filter(File::exists).findAny().orElse(candidates.get(0));
  }

  private List<File> getFilesFor(String key, IndexType<?, ?, ?> indexType) {
    return indexType.getFileExtensions(null).stream()
        .map(fileExtension -> new File(_tmpMapIndexDir, key + fileExtension)).collect(Collectors.toList());
  }

  private String allocationContext(File f, String context) {
    return this.getClass().getSimpleName() + "." + f.toString() + "." + context;
  }

  static FieldSpec.DataType convertToDataType(PinotDataType ty) {
    // TODO: I've been told that we already have a function to do this, so find that function and replace this
    switch (ty) {
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case SHORT:
      case INTEGER:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BIG_DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case STRING:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static Comparable<?> getPlaceholderValue(DataType type) {
    // TODO(ERICH): is this still needed?
    switch (type) {
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0F;
      case DOUBLE:
        return 0.0D;
      case BOOLEAN:
        return false;
      case STRING:
        return "null";
      case BIG_DECIMAL:
      case TIMESTAMP:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException();
    }
  }
}
