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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OnHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.creator.TextIndexType;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment.*;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  // Allow at most 512 characters for the metadata property
  private static final int METADATA_PROPERTY_LENGTH_LIMIT = 512;

  private SegmentGeneratorConfig _config;
  private Map<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private final Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  private final Map<String, ForwardIndexCreator> _forwardIndexCreatorMap = new HashMap<>();
  private final Map<String, DictionaryBasedInvertedIndexCreator> _invertedIndexCreatorMap = new HashMap<>();
  private final Map<String, TextIndexCreator> _textIndexCreatorMap = new HashMap<>();
  private final Map<String, TextIndexCreator> _fstIndexCreatorMap = new HashMap<>();
  private final Map<String, JsonIndexCreator> _jsonIndexCreatorMap = new HashMap<>();
  private final Map<String, GeoSpatialIndexCreator> _h3IndexCreatorMap = new HashMap<>();
  private final Map<String, NullValueVectorCreator> _nullValueVectorCreatorMap = new HashMap<>();
  private String _segmentName;
  private Schema _schema;
  private File _indexDir;
  private int _totalDocs;
  private int _docIdCounter;
  private boolean _nullHandlingEnabled;
  private Map<String, Map<String, String>> _columnProperties;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir)
      throws Exception {
    _docIdCounter = 0;
    _config = segmentCreationSpec;
    _indexCreationInfoMap = indexCreationInfoMap;
    _columnProperties = segmentCreationSpec.getColumnProperties();

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    _schema = schema;
    _totalDocs = segmentIndexCreationInfo.getTotalDocs();
    if (_totalDocs == 0) {
      return;
    }

    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    Set<String> invertedIndexColumns = new HashSet<>();
    for (String columnName : _config.getInvertedIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create inverted index for column: %s because it is not in schema", columnName);
      invertedIndexColumns.add(columnName);
    }

    Set<String> textIndexColumns = new HashSet<>();
    for (String columnName : _config.getTextIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create text index for column: %s because it is not in schema", columnName);
      textIndexColumns.add(columnName);
    }

    Set<String> fstIndexColumns = new HashSet<>();
    for (String columnName : _config.getFSTIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create FST index for column: %s because it is not in schema", columnName);
      fstIndexColumns.add(columnName);
    }

    Set<String> jsonIndexColumns = new HashSet<>();
    for (String columnName : _config.getJsonIndexCreationColumns()) {
      Preconditions.checkState(schema.hasColumn(columnName),
          "Cannot create text index for column: %s because it is not in schema", columnName);
      jsonIndexColumns.add(columnName);
    }

    Map<String, H3IndexConfig> h3IndexConfigs = _config.getH3IndexConfigs();
    for (String columnName : h3IndexConfigs.keySet()) {
      Preconditions
          .checkState(schema.hasColumn(columnName), "Cannot create H3 index for column: %s because it is not in schema",
              columnName);
    }

    // Initialize creators for dictionary, forward index and inverted index
    for (FieldSpec fieldSpec : fieldSpecs) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      DataType storedType = fieldSpec.getDataType().getStoredType();
      ColumnIndexCreationInfo indexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(indexCreationInfo, "Missing index creation info for column: %s", columnName);
      boolean dictEnabledColumn = createDictionaryForColumn(indexCreationInfo, segmentCreationSpec, fieldSpec);

      if (dictEnabledColumn) {
        // Create dictionary-encoded index

        // Initialize dictionary creator
        SegmentDictionaryCreator dictionaryCreator =
            new SegmentDictionaryCreator(indexCreationInfo.getSortedUniqueElementsArray(), fieldSpec, _indexDir,
                indexCreationInfo.isUseVarLengthDictionary());
        _dictionaryCreatorMap.put(columnName, dictionaryCreator);

        // Create dictionary
        try {
          dictionaryCreator.build();
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              fieldSpec.getName(), indexCreationInfo.getDistinctValueCount(), dictionaryCreator.getNumBytesPerEntry());
          throw e;
        }

        // Initialize forward index creator
        int cardinality = indexCreationInfo.getDistinctValueCount();
        if (fieldSpec.isSingleValueField()) {
          if (indexCreationInfo.isSorted()) {
            _forwardIndexCreatorMap
                .put(columnName, new SingleValueSortedForwardIndexCreator(_indexDir, columnName, cardinality));
          } else {
            _forwardIndexCreatorMap.put(columnName,
                new SingleValueUnsortedForwardIndexCreator(_indexDir, columnName, cardinality, _totalDocs));
          }
        } else {
          _forwardIndexCreatorMap.put(columnName,
              new MultiValueUnsortedForwardIndexCreator(_indexDir, columnName, cardinality, _totalDocs,
                  indexCreationInfo.getTotalNumberOfEntries()));
        }

        // Initialize inverted index creator; skip creating inverted index if sorted
        if (invertedIndexColumns.contains(columnName) && !indexCreationInfo.isSorted()) {
          if (segmentCreationSpec.isOnHeap()) {
            _invertedIndexCreatorMap
                .put(columnName, new OnHeapBitmapInvertedIndexCreator(_indexDir, columnName, cardinality));
          } else {
            _invertedIndexCreatorMap.put(columnName,
                new OffHeapBitmapInvertedIndexCreator(_indexDir, fieldSpec, cardinality, _totalDocs,
                    indexCreationInfo.getTotalNumberOfEntries()));
          }
        }
      } else {
        // Create raw index
        Preconditions.checkState(!invertedIndexColumns.contains(columnName),
            "Cannot create inverted index for raw index column: %s", columnName);

        ChunkCompressionType compressionType = getColumnCompressionType(segmentCreationSpec, fieldSpec);

        // Initialize forward index creator
        boolean deriveNumDocsPerChunk =
            shouldDeriveNumDocsPerChunk(columnName, segmentCreationSpec.getColumnProperties());
        int writerVersion = rawIndexWriterVersion(columnName, segmentCreationSpec.getColumnProperties());
        if (fieldSpec.isSingleValueField()) {
          _forwardIndexCreatorMap.put(columnName,
              getRawIndexCreatorForSVColumn(_indexDir, compressionType, columnName, storedType, _totalDocs,
                  indexCreationInfo.getLengthOfLongestEntry(), deriveNumDocsPerChunk, writerVersion));
        } else {
          _forwardIndexCreatorMap.put(columnName,
              getRawIndexCreatorForMVColumn(_indexDir, compressionType, columnName, storedType, _totalDocs,
                  indexCreationInfo.getMaxNumberOfMultiValueElements(), deriveNumDocsPerChunk, writerVersion,
                  indexCreationInfo.getMaxRowLengthInBytes()));
        }
      }

      if (textIndexColumns.contains(columnName)) {
        // Initialize text index creator
        Preconditions
            .checkState(storedType == DataType.STRING, "Text index is currently only supported on STRING type columns");
        _textIndexCreatorMap
            .put(columnName, new LuceneTextIndexCreator(columnName, _indexDir, true /* commitOnClose */));
      }

      if (fstIndexColumns.contains(columnName)) {
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            "FST index is currently only supported on single-value columns");
        Preconditions
            .checkState(storedType == DataType.STRING, "FST index is currently only supported on STRING type columns");
        Preconditions
            .checkState(dictEnabledColumn, "FST index is currently only supported on dictionary-encoded columns");
        _fstIndexCreatorMap.put(columnName, new LuceneFSTIndexCreator(_indexDir, columnName,
            (String[]) indexCreationInfo.getSortedUniqueElementsArray()));
      }

      if (jsonIndexColumns.contains(columnName)) {
        Preconditions.checkState(fieldSpec.isSingleValueField(),
            "Json index is currently only supported on single-value columns");
        Preconditions
            .checkState(storedType == DataType.STRING, "Json index is currently only supported on STRING columns");
        JsonIndexCreator jsonIndexCreator =
            segmentCreationSpec.isOnHeap() ? new OnHeapJsonIndexCreator(_indexDir, columnName)
                : new OffHeapJsonIndexCreator(_indexDir, columnName);
        _jsonIndexCreatorMap.put(columnName, jsonIndexCreator);
      }

      H3IndexConfig h3IndexConfig = h3IndexConfigs.get(columnName);
      if (h3IndexConfig != null) {
        Preconditions
            .checkState(fieldSpec.isSingleValueField(), "H3 index is currently only supported on single-value columns");
        Preconditions.checkState(storedType == DataType.BYTES, "H3 index is currently only supported on BYTES columns");
        H3IndexResolution resolution = h3IndexConfig.getResolution();
        GeoSpatialIndexCreator h3IndexCreator =
            segmentCreationSpec.isOnHeap() ? new OnHeapH3IndexCreator(_indexDir, columnName, resolution)
                : new OffHeapH3IndexCreator(_indexDir, columnName, resolution);
        _h3IndexCreatorMap.put(columnName, h3IndexCreator);
      }

      _nullHandlingEnabled = _config.isNullHandlingEnabled();
      if (_nullHandlingEnabled) {
        // Initialize Null value vector map
        _nullValueVectorCreatorMap.put(columnName, new NullValueVectorCreator(_indexDir, columnName));
      }
    }
  }

  public static boolean shouldDeriveNumDocsPerChunk(String columnName,
      Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      return properties != null && Boolean
          .parseBoolean(properties.get(FieldConfig.DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY));
    }
    return false;
  }

  public static int rawIndexWriterVersion(String columnName, Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null && columnProperties.get(columnName) != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      String version = properties.get(FieldConfig.RAW_INDEX_WRITER_VERSION);
      if (version == null) {
        return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
      }
      return Integer.parseInt(version);
    }
    return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
  }

  /**
   * Helper method that returns compression type to use based on segment creation spec and field type.
   * <ul>
   *   <li> Returns compression type from segment creation spec, if specified there.</li>
   *   <li> Else, returns PASS_THROUGH for metrics, and SNAPPY for dimensions. This is because metrics are likely
   *        to be spread in different chunks after applying predicates. Same could be true for dimensions, but in that
   *        case, clients are expected to explicitly specify the appropriate compression type in the spec. </li>
   * </ul>
   * @param segmentCreationSpec Segment creation spec
   * @param fieldSpec Field spec for the column
   * @return Compression type to use
   */
  private ChunkCompressionType getColumnCompressionType(SegmentGeneratorConfig segmentCreationSpec,
      FieldSpec fieldSpec) {
    ChunkCompressionType compressionType = segmentCreationSpec.getRawIndexCompressionType().get(fieldSpec.getName());
    if (compressionType == null) {
      if (fieldSpec.getFieldType() == FieldType.METRIC) {
        return ChunkCompressionType.PASS_THROUGH;
      } else {
        return ChunkCompressionType.SNAPPY;
      }
    } else {
      return compressionType;
    }
  }

  /**
   * Returns true if dictionary should be created for a column, false otherwise.
   * Currently there are two sources for this config:
   * <ul>
   *   <li> ColumnIndexCreationInfo (this is currently hard-coded to always return dictionary). </li>
   *   <li> SegmentGeneratorConfig</li>
   * </ul>
   *
   * This method gives preference to the SegmentGeneratorConfig first.
   *
   * @param info Column index creation info
   * @param config Segment generation config
   * @param spec Field spec for the column
   * @return True if dictionary should be created for the column, false otherwise
   */
  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    String column = spec.getName();
    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      return false;
    }
    return info.isCreateDictionary();
  }

  @Override
  public void indexRow(GenericRow row)
      throws IOException {
    for (Map.Entry<String, ForwardIndexCreator> entry : _forwardIndexCreatorMap.entrySet()) {
      String columnName = entry.getKey();
      ForwardIndexCreator forwardIndexCreator = entry.getValue();

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);

      //get dictionaryCreator, will be null if column is not dictionaryEncoded
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);

      // text-index
      TextIndexCreator textIndexCreator = _textIndexCreatorMap.get(columnName);
      if (textIndexCreator != null) {
        if (fieldSpec.isSingleValueField()) {
          textIndexCreator.add((String) columnValueToIndex);
        } else {
          textIndexCreator.add((String[]) columnValueToIndex, ((String[]) columnValueToIndex).length);
        }
      }

      if (fieldSpec.isSingleValueField()) {
        // Single Value column
        JsonIndexCreator jsonIndexCreator = _jsonIndexCreatorMap.get(columnName);
        if (jsonIndexCreator != null) {
          jsonIndexCreator.add((String) columnValueToIndex);
        }
        GeoSpatialIndexCreator h3IndexCreator = _h3IndexCreatorMap.get(columnName);
        if (h3IndexCreator != null) {
          h3IndexCreator.add(GeometrySerializer.deserialize((byte[]) columnValueToIndex));
        }
        if (dictionaryCreator != null) {
          // dictionary encoded SV column
          // get dictID from dictionary
          int dictId = dictionaryCreator.indexOfSV(columnValueToIndex);
          // store the docID -> dictID mapping in forward index
          forwardIndexCreator.putDictId(dictId);
          DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap.get(columnName);
          if (invertedIndexCreator != null) {
            // if inverted index enabled during segment creation,
            // then store dictID -> docID mapping in inverted index
            invertedIndexCreator.add(dictId);
          }
        } else {
          // non-dictionary encoded SV column
          // store the docId -> raw value mapping in forward index
          if (textIndexCreator != null && !shouldStoreRawValueForTextIndex(columnName)) {
            // for text index on raw columns, check the config to determine if actual raw value should
            // be stored or not
            columnValueToIndex = _columnProperties.get(columnName).get(FieldConfig.TEXT_INDEX_RAW_VALUE);
            if (columnValueToIndex == null) {
              columnValueToIndex = FieldConfig.TEXT_INDEX_DEFAULT_RAW_VALUE;
            }
          }
          switch (forwardIndexCreator.getValueType()) {
            case INT:
              forwardIndexCreator.putInt((int) columnValueToIndex);
              break;
            case LONG:
              forwardIndexCreator.putLong((long) columnValueToIndex);
              break;
            case FLOAT:
              forwardIndexCreator.putFloat((float) columnValueToIndex);
              break;
            case DOUBLE:
              forwardIndexCreator.putDouble((double) columnValueToIndex);
              break;
            case STRING:
              forwardIndexCreator.putString((String) columnValueToIndex);
              break;
            case BYTES:
              forwardIndexCreator.putBytes((byte[]) columnValueToIndex);
              break;
            default:
              throw new IllegalStateException();
          }
        }
      } else {
        if (dictionaryCreator != null) {
          //dictionary encoded
          int[] dictIds = dictionaryCreator.indexOfMV(columnValueToIndex);
          forwardIndexCreator.putDictIdMV(dictIds);
          DictionaryBasedInvertedIndexCreator invertedIndexCreator = _invertedIndexCreatorMap
              .get(columnName);
          if (invertedIndexCreator != null) {
            invertedIndexCreator.add(dictIds, dictIds.length);
          }
        } else {
          // for text index on raw columns, check the config to determine if actual raw value should
          // be stored or not
          if (textIndexCreator != null && !shouldStoreRawValueForTextIndex(columnName)) {
            Object value = _columnProperties.get(columnName)
                .get(FieldConfig.TEXT_INDEX_RAW_VALUE);
            if (value == null) {
              value = FieldConfig.TEXT_INDEX_DEFAULT_RAW_VALUE;
            }
            if (forwardIndexCreator.getValueType().getStoredType() == DataType.STRING) {
              columnValueToIndex = new String[] {String.valueOf(value)};
            } else if (forwardIndexCreator.getValueType().getStoredType() == DataType.BYTES) {
              columnValueToIndex = new byte[][] {String.valueOf(value).getBytes(UTF_8)};
            } else {
              throw new RuntimeException("Text Index is only supported for STRING and BYTES stored type");
            }
          }
          switch (forwardIndexCreator.getValueType()) {
            case INT:
              if (columnValueToIndex instanceof Object[]) {
                int[] array = new int[((Object[]) columnValueToIndex).length];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (Integer) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putIntMV(array);
              }
              break;
            case LONG:
              if (columnValueToIndex instanceof Object[]) {
                long[] array = new long[((Object[]) columnValueToIndex).length];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (Long) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putLongMV(array);
              }
              break;
            case FLOAT:
              if (columnValueToIndex instanceof Object[]) {
                float[] array = new float[((Object[]) columnValueToIndex).length];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (Float) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putFloatMV(array);
              }
              break;
            case DOUBLE:
              if (columnValueToIndex instanceof Object[]) {
                double[] array = new double[((Object[]) columnValueToIndex).length];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (Double) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putDoubleMV(array);
              }
              break;
            case STRING:
              if (columnValueToIndex instanceof String[]) {
                forwardIndexCreator.putStringMV((String[]) columnValueToIndex);
              } else if (columnValueToIndex instanceof Object[]) {
                String[] array = new String[((Object[]) columnValueToIndex).length];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (String) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putStringMV(array);
              }
              break;
            case BYTES:
              if (columnValueToIndex instanceof byte[][]) {
                forwardIndexCreator.putBytesMV((byte[][]) columnValueToIndex);
              } else if (columnValueToIndex instanceof Object[]) {
                byte[][] array = new byte[((Object[]) columnValueToIndex).length][];
                for (int i = 0; i < array.length; i++) {
                  array[i] = (byte[]) ((Object[]) columnValueToIndex)[i];
                }
                forwardIndexCreator.putBytesMV(array);
              }
              break;
            default:
              throw new IllegalStateException();
          }
        }
      }

      if (_nullHandlingEnabled) {
        // If row has null value for given column name, add to null value vector
        if (row.isNullValue(columnName)) {
          _nullValueVectorCreatorMap.get(columnName).setNull(_docIdCounter);
        }
      }
    }
    _docIdCounter++;
  }

  private boolean shouldStoreRawValueForTextIndex(String column) {
    if (_columnProperties != null) {
      Map<String, String> props = _columnProperties.get(column);
      // by default always store the raw value
      // if the config is set to true, don't store the actual raw value
      // there will be a dummy value
      return props == null || !Boolean.parseBoolean(props.get(FieldConfig.TEXT_INDEX_NO_RAW_DATA));
    }

    return true;
  }

  @Override
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public void seal()
      throws ConfigurationException, IOException {
    for (DictionaryBasedInvertedIndexCreator invertedIndexCreator : _invertedIndexCreatorMap.values()) {
      invertedIndexCreator.seal();
    }
    for (TextIndexCreator textIndexCreator : _textIndexCreatorMap.values()) {
      textIndexCreator.seal();
    }
    for (TextIndexCreator fstIndexCreator : _fstIndexCreatorMap.values()) {
      fstIndexCreator.seal();
    }
    for (JsonIndexCreator jsonIndexCreator : _jsonIndexCreatorMap.values()) {
      jsonIndexCreator.seal();
    }
    for (GeoSpatialIndexCreator h3IndexCreator : _h3IndexCreatorMap.values()) {
      h3IndexCreator.seal();
    }
    for (NullValueVectorCreator nullValueVectorCreator : _nullValueVectorCreatorMap.values()) {
      nullValueVectorCreator.seal();
    }
    writeMetadata();
  }

  private void writeMetadata()
      throws ConfigurationException {
    PropertiesConfiguration properties =
        new PropertiesConfiguration(new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

    properties.setProperty(SEGMENT_CREATOR_VERSION, _config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, _segmentName);
    properties.setProperty(TABLE_NAME, _config.getTableName());
    properties.setProperty(DIMENSIONS, _config.getDimensions());
    properties.setProperty(METRICS, _config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, _config.getDateTimeColumnNames());
    String timeColumnName = _config.getTimeColumnName();
    properties.setProperty(TIME_COLUMN_NAME, timeColumnName);
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(_totalDocs));

    // Write time related metadata (start time, end time, time unit)
    if (timeColumnName != null) {
      ColumnIndexCreationInfo timeColumnIndexCreationInfo = _indexCreationInfoMap.get(timeColumnName);
      if (timeColumnIndexCreationInfo != null) {
        long startTime;
        long endTime;
        TimeUnit timeUnit;

        // Use start/end time in config if defined
        if (_config.getStartTime() != null) {
          startTime = Long.parseLong(_config.getStartTime());
          endTime = Long.parseLong(_config.getEndTime());
          timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
        } else {
          if (_totalDocs > 0) {
            String startTimeStr = timeColumnIndexCreationInfo.getMin().toString();
            String endTimeStr = timeColumnIndexCreationInfo.getMax().toString();

            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              // For TimeColumnType.SIMPLE_DATE_FORMAT, convert time value into millis since epoch
              DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(_config.getSimpleDateFormat());
              startTime = dateTimeFormatter.parseMillis(startTimeStr);
              endTime = dateTimeFormatter.parseMillis(endTimeStr);
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              // by default, time column type is TimeColumnType.EPOCH
              startTime = Long.parseLong(startTimeStr);
              endTime = Long.parseLong(endTimeStr);
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
            }
          } else {
            // No records in segment. Use current time as start/end
            long now = System.currentTimeMillis();
            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              startTime = now;
              endTime = now;
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
              startTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
              endTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
            }
          }
        }

        if (!_config.isSkipTimeValueCheck()) {
          Interval timeInterval =
              new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime), DateTimeZone.UTC);
          Preconditions.checkState(TimeUtils.isValidTimeInterval(timeInterval),
              "Invalid segment start/end time: %s (in millis: %s/%s) for time column: %s, must be between: %s",
              timeInterval, timeInterval.getStartMillis(), timeInterval.getEndMillis(), timeColumnName,
              TimeUtils.VALID_TIME_INTERVAL);
        }

        properties.setProperty(SEGMENT_START_TIME, startTime);
        properties.setProperty(SEGMENT_END_TIME, endTime);
        properties.setProperty(TIME_UNIT, timeUnit);
      }
    }

    for (Map.Entry<String, String> entry : _config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : _indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(column);
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getNumBytesPerEntry() : 0;

      // TODO: after fixing the server-side dependency on HAS_INVERTED_INDEX and deployed, set HAS_INVERTED_INDEX
      //  properly
      // The hasInvertedIndex flag in segment metadata is picked up in ColumnMetadata, and will be used during the query
      // plan phase. If it is set to false, then inverted indexes are not used in queries even if they are created
      // via table
      // configs on segment load. So, we set it to true here for now, until we fix the server to update the value inside
      // ColumnMetadata, export information to the query planner that the inverted index available is current and can
      // be used.
      //
      //    boolean hasInvertedIndex = invertedIndexCreatorMap.containsKey();
      boolean hasInvertedIndex = true;

      // for new generated segment we write as NONE if text index does not exist
      // for reading existing segments that don't have this property, non-existence
      // of this property will be treated as NONE. See the builder in ColumnMetadata
      TextIndexType textIndexType =
          _textIndexCreatorMap.containsKey(column) ? TextIndexType.LUCENE : TextIndexType.NONE;

      boolean hasFSTIndex = _fstIndexCreatorMap.containsKey(column);

      boolean hasJsonIndex = _jsonIndexCreatorMap.containsKey(column);

      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, _totalDocs, _schema.getFieldSpecFor(column),
          _dictionaryCreatorMap.containsKey(column), dictionaryElementSize, hasInvertedIndex, textIndexType,
          hasFSTIndex, hasJsonIndex);
    }

    properties.save();
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, FieldSpec fieldSpec, boolean hasDictionary,
      int dictionaryElementSize, boolean hasInvertedIndex, TextIndexType textIndexType, boolean hasFSTIndex,
      boolean hasJsonIndex) {
    int cardinality = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_NULL_VALUE), String.valueOf(columnIndexCreationInfo.hasNulls()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(getKeyFor(column, TEXT_INDEX_TYPE), textIndexType.name());
    properties.setProperty(getKeyFor(column, HAS_INVERTED_INDEX), String.valueOf(hasInvertedIndex));
    properties.setProperty(getKeyFor(column, HAS_FST_INDEX), String.valueOf(hasFSTIndex));
    properties.setProperty(getKeyFor(column, HAS_JSON_INDEX), String.valueOf(hasJsonIndex));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMENTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties
        .setProperty(getKeyFor(column, IS_AUTO_GENERATED), String.valueOf(columnIndexCreationInfo.isAutoGenerated()));

    PartitionFunction partitionFunction = columnIndexCreationInfo.getPartitionFunction();
    if (partitionFunction != null) {
      properties.setProperty(getKeyFor(column, PARTITION_FUNCTION), partitionFunction.toString());
      properties.setProperty(getKeyFor(column, NUM_PARTITIONS), columnIndexCreationInfo.getNumPartitions());
      properties.setProperty(getKeyFor(column, PARTITION_VALUES), columnIndexCreationInfo.getPartitions());
    }

    // datetime field
    if (fieldSpec.getFieldType().equals(FieldType.DATE_TIME)) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    // NOTE: Min/max could be null for real-time aggregate metrics.
    if (totalDocs > 0) {
      Object min = columnIndexCreationInfo.getMin();
      Object max = columnIndexCreationInfo.getMax();
      if (min != null && max != null) {
        addColumnMinMaxValueInfo(properties, column, min.toString(), max.toString());
      }
    }

    String defaultNullValue = columnIndexCreationInfo.getDefaultNullValue().toString();
    if (isValidPropertyValue(defaultNullValue)) {
      properties.setProperty(getKeyFor(column, DEFAULT_NULL_VALUE), defaultNullValue);
    }
  }

  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column, String minValue,
      String maxValue) {
    if (isValidPropertyValue(minValue)) {
      properties.setProperty(getKeyFor(column, MIN_VALUE), minValue);
    }
    if (isValidPropertyValue(maxValue)) {
      properties.setProperty(getKeyFor(column, MAX_VALUE), maxValue);
    }
  }

  /**
   * Helper method to check whether the given value is a valid property value.
   * <p>Value is invalid iff:
   * <ul>
   *   <li>It contains more than 512 characters</li>
   *   <li>It contains leading/trailing whitespace</li>
   *   <li>It contains list separator (',')</li>
   * </ul>
   */
  @VisibleForTesting
  static boolean isValidPropertyValue(String value) {
    int length = value.length();
    if (length == 0) {
      return true;
    }
    if (length > METADATA_PROPERTY_LENGTH_LIMIT) {
      return false;
    }
    if (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(length - 1))) {
      return false;
    }
    return value.indexOf(',') == -1;
  }

  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.subset(COLUMN_PROPS_KEY_PREFIX + column).clear();
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param lengthOfLongestEntry Length of longest entry
   * @param deriveNumDocsPerChunk true if varbyte writer should auto-derive the number of rows per chunk
   * @param writerVersion version to use for the raw index writer
   * @return raw index creator
   */
  public static ForwardIndexCreator getRawIndexCreatorForSVColumn(File file, ChunkCompressionType compressionType,
      String column, DataType dataType, int totalDocs, int lengthOfLongestEntry, boolean deriveNumDocsPerChunk,
      int writerVersion)
      throws IOException {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            writerVersion);
      case STRING:
      case BYTES:
        return new SingleValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            lengthOfLongestEntry, deriveNumDocsPerChunk, writerVersion);
      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param deriveNumDocsPerChunk true if varbyte writer should auto-derive the number of rows
   *     per chunk
   * @param writerVersion version to use for the raw index writer
   * @param maxRowLengthInBytes the length of the longest row in bytes
   * @return raw index creator
   */
  public static ForwardIndexCreator getRawIndexCreatorForMVColumn(File file, ChunkCompressionType compressionType,
      String column, DataType dataType, final int totalDocs, int maxNumberOfMultiValueElements,
      boolean deriveNumDocsPerChunk, int writerVersion, int maxRowLengthInBytes)
      throws IOException {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new MultiValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            maxNumberOfMultiValueElements, deriveNumDocsPerChunk, writerVersion);
      case STRING:
      case BYTES:
        return new MultiValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, dataType, writerVersion,
            maxRowLengthInBytes, maxNumberOfMultiValueElements);
      default:
        throw new UnsupportedOperationException(
            "Data type not supported for raw indexing: " + dataType);
    }
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.close(Iterables
        .concat(_dictionaryCreatorMap.values(), _forwardIndexCreatorMap.values(), _invertedIndexCreatorMap.values(),
            _textIndexCreatorMap.values(), _fstIndexCreatorMap.values(), _jsonIndexCreatorMap.values(),
            _h3IndexCreatorMap.values(), _nullValueVectorCreatorMap.values()));
  }
}
