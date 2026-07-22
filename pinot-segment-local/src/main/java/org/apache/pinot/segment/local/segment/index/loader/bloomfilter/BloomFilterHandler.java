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
package org.apache.pinot.segment.local.segment.index.loader.bloomfilter;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.readers.bloom.GuavaBloomFilterReaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BloomFilterHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterHandler.class);

  private static final int TYPE_VALUE_OFFSET = 0;
  private static final int VERSION_OFFSET = 4;
  // In V1 format the Guava payload starts at byte 8. Within that payload the layout is:
  //   [strategy (1 byte)][numHashFunctions (1 byte)][numLongs (int, 4 bytes)][bit array...]
  // These absolute file offsets match BaseGuavaBloomFilterReader's relative offsets + 8.
  private static final int V1_NUM_HASH_FUNCTIONS_OFFSET = 9;  // 4 (TYPE) + 4 (VERSION) + 1 (strategy); reads 1 byte
  private static final int V1_NUM_LONGS_OFFSET = 10;          // + 1 (numHashFunctions); reads 4-byte int

  private final Map<String, BloomFilterConfig> _bloomFilterConfigs;

  public BloomFilterHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _bloomFilterConfigs = FieldIndexConfigsUtil.enableConfigByColumn(StandardIndexes.bloomFilter(), fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddBF = new HashSet<>(_bloomFilterConfigs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.bloomFilter());
    // Check if any existing bloomfilter need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddBF.remove(column)) {
        LOGGER.info("Need to remove existing bloom filter from segment: {}, column: {}", segmentName, column);
        return true;
      } else {
        // Bloom filter exists for this column; check if fpp config changed by comparing numHashFunctions
        if (isFppChanged(segmentReader, segmentName, column)) {
          return true;
        }
      }
    }
    // Check if any new bloomfilter need to be added.
    for (String column : columnsToAddBF) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateBloomFilter(columnMetadata)) {
        LOGGER.info("Need to create new bloom filter for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether the effective fpp config has changed for an existing bloom filter index.
   *
   * <p>V2 segments (VERSION_V2 = 2) store the effective fpp explicitly in the header; the comparison is exact.
   * V2 segments were created during a short window before the format was rolled back for backward compatibility;
   * V2 reading is retained so those segments are not permanently stuck.
   *
   * <p>V1 segments (VERSION = 1, the current write format) do not store fpp in the header. FPP change is
   * detected structurally: the stored {@code numHashFunctions} and {@code numLongs} in the Guava payload
   * are compared to the values that would be produced by the configured fpp + cardinality. These two fields
   * jointly encode the filter capacity and are stable across Guava versions.
   *
   * <p>Accepts both Reader and Writer since Writer extends Reader, allowing this method to be called from
   * both needUpdateIndices and updateIndices.
   */
  private boolean isFppChanged(SegmentDirectory.Reader segmentReader, String segmentName, String column) {
    try {
      PinotDataBuffer dataBuffer = segmentReader.getIndexFor(column, StandardIndexes.bloomFilter());
      int version = dataBuffer.getInt(VERSION_OFFSET);
      BloomFilterConfig config = _bloomFilterConfigs.get(column);
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);

      if (version == OnHeapGuavaBloomFilterCreator.VERSION_V2) {
        // V2 header stores the effective fpp explicitly.
        double storedFpp = dataBuffer.getDouble(OnHeapGuavaBloomFilterCreator.FPP_OFFSET);
        double expectedFpp = computeEffectiveFpp(columnMetadata, config);
        if (Double.compare(storedFpp, expectedFpp) != 0) {
          LOGGER.info("Bloom filter fpp config changed for segment: {}, column: {}, stored fpp: {}, "
                  + "expected fpp: {}. Index needs to be rebuilt.",
              segmentName, column, storedFpp, expectedFpp);
          return true;
        }
        return false;
      }

      if (version != OnHeapGuavaBloomFilterCreator.VERSION) {
        // Unrecognized version — force rebuild rather than silently reading bytes at wrong offsets.
        LOGGER.warn("Unrecognized bloom filter version {} for segment: {}, column: {}; forcing rebuild.",
            version, segmentName, column);
        return true;
      }
      // V1 format: detect fpp change via numHashFunctions + numLongs in the Guava payload.
      // These values are at fixed offsets (see V1_NUM_HASH_FUNCTIONS_OFFSET / V1_NUM_LONGS_OFFSET).
      int storedNumHashFunctions = dataBuffer.getByte(V1_NUM_HASH_FUNCTIONS_OFFSET) & 0xFF;
      int storedNumLongs = dataBuffer.getInt(V1_NUM_LONGS_OFFSET);
      int[] expected = computeExpectedNumHashFunctionsAndNumLongs(columnMetadata, config);
      int expectedNumHashFunctions = expected[0];
      int expectedNumLongs = expected[1];
      if (storedNumHashFunctions != expectedNumHashFunctions || storedNumLongs != expectedNumLongs) {
        LOGGER.info("Bloom filter fpp config changed for segment: {}, column: {}, "
                + "stored numHashFunctions: {}, expected: {}, stored numLongs: {}, expected: {}. "
                + "Index needs to be rebuilt.",
            segmentName, column, storedNumHashFunctions, expectedNumHashFunctions, storedNumLongs, expectedNumLongs);
        return true;
      }
      return false;
    } catch (Exception e) {
      LOGGER.warn("Failed to read existing bloom filter for segment: {}, column: {}", segmentName, column, e);
      return false;
    }
  }

  /**
   * Computes the {@code numHashFunctions} and {@code numLongs} that Guava would allocate for the given
   * fpp and cardinality, matching the formulas in Guava's {@code BloomFilter}.
   *
   * <p>{@code numLongs} uses {@code optimalNumOfBits(n, p) = (long)(-n * ln(p) / (ln 2)^2)}.
   * {@code numHashFunctions} uses the direct formula {@code max(1, round(-ln(p) / ln(2)))},
   * which is what current Guava (33+) writes. The older two-step path via {@code idealNumBits / n * ln(2)}
   * diverges for small cardinalities due to long-cast truncation and must not be used.
   *
   * @return {@code int[]{numHashFunctions, numLongs}}
   */
  private int[] computeExpectedNumHashFunctionsAndNumLongs(ColumnMetadata columnMetadata,
      BloomFilterConfig bloomFilterConfig) {
    double fpp = computeEffectiveFpp(columnMetadata, bloomFilterConfig);
    int cardinality = columnMetadata != null ? columnMetadata.getCardinality() : 1;
    if (cardinality <= 0) {
      cardinality = columnMetadata != null ? columnMetadata.getTotalNumberOfEntries() : 1;
    }
    if (cardinality <= 0) {
      cardinality = 1;
    }
    // Guava: optimalNumOfBits = -n * ln(p) / (ln 2)^2, minimum 1
    long idealNumBits = Math.max(1, (long) (-cardinality * Math.log(fpp) / (Math.log(2) * Math.log(2))));
    // Guava: numLongs = ceil(idealNumBits / 64).
    int numLongs = (int) ((idealNumBits + 63) / 64);
    // Guava (33+): optimalNumOfHashFunctions(p) = max(1, round(-ln(p) / ln(2)))
    // This is the direct formula Guava now uses; it avoids the long-cast quantization error
    // in the old two-step path (via idealNumBits / n) that diverges for small cardinalities.
    int numHashFunctions = Math.max(1, (int) Math.round(-Math.log(fpp) / Math.log(2)));
    return new int[]{numHashFunctions, numLongs};
  }

  /**
   * Computes the effective fpp for a new bloom filter: applies the {@code maxSizeInBytes} cap if set.
   * This matches the fpp computation in {@link OnHeapGuavaBloomFilterCreator}.
   */
  private double computeEffectiveFpp(ColumnMetadata columnMetadata, BloomFilterConfig bloomFilterConfig) {
    double fpp = bloomFilterConfig.getFpp();
    int maxSizeInBytes = bloomFilterConfig.getMaxSizeInBytes();
    if (maxSizeInBytes > 0 && columnMetadata != null) {
      int cardinality = columnMetadata.getCardinality();
      if (cardinality <= 0) {
        cardinality = columnMetadata.getTotalNumberOfEntries();
      }
      if (cardinality > 0) {
        double minFpp = GuavaBloomFilterReaderUtils.computeFPP(maxSizeInBytes, cardinality);
        fpp = Math.max(fpp, minFpp);
      }
    }
    return fpp;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    Set<String> columnsToAddBF = new HashSet<>(_bloomFilterConfigs.keySet());
    // Remove indices not set in table config any more.
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.bloomFilter());
    for (String column : existingColumns) {
      if (!columnsToAddBF.remove(column)) {
        LOGGER.info("Removing existing bloom filter from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.bloomFilter());
        LOGGER.info("Removed existing bloom filter from segment: {}, column: {}", segmentName, column);
      } else {
        // Bloom filter exists for this column; check if fpp config changed
        if (isFppChanged(segmentWriter, segmentName, column)) {
          LOGGER.info("Deleting existing bloom filter for segment: {}, column: {} before rebuilding.", segmentName,
              column);
          segmentWriter.removeIndex(column, StandardIndexes.bloomFilter());
          LOGGER.info("Removed existing bloom filter from segment: {}, column: {}", segmentName, column);
          columnsToAddBF.add(column);
        }
      }
    }
    for (String column : columnsToAddBF) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateBloomFilter(columnMetadata)) {
        createBloomFilterForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private boolean shouldCreateBloomFilter(ColumnMetadata columnMetadata) {
    return columnMetadata != null;
  }

  private void createAndSealBloomFilterForDictionaryColumn(File indexDir, ColumnMetadata columnMetadata,
      BloomFilterConfig bloomFilterConfig, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    IndexCreationContext context = new IndexCreationContext.Builder(indexDir, _tableConfig, columnMetadata).build();
    try (BloomFilterCreator bloomFilterCreator =
        StandardIndexes.bloomFilter().createIndexCreator(context, bloomFilterConfig);
        Dictionary dictionary = getDictionaryReader(columnMetadata, segmentWriter)) {
      int length = dictionary.length();
      for (int i = 0; i < length; i++) {
        bloomFilterCreator.add(dictionary.getStringValue(i));
      }
      bloomFilterCreator.seal();
    }
  }

  private void createAndSealBloomFilterForNonDictionaryColumn(File indexDir, ColumnMetadata columnMetadata,
      BloomFilterConfig bloomFilterConfig, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    int numDocs = columnMetadata.getTotalDocs();
    IndexCreationContext context = new IndexCreationContext.Builder(indexDir, _tableConfig, columnMetadata).build();
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (BloomFilterCreator bloomFilterCreator = StandardIndexes.bloomFilter()
        .createIndexCreator(context, bloomFilterConfig);
        ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
            _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
      if (columnMetadata.isSingleValue()) {
        // SV
        switch (columnMetadata.getDataType()) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(Integer.toString(forwardIndexReader.getInt(i, readerContext)));
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(Long.toString(forwardIndexReader.getLong(i, readerContext)));
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(Float.toString(forwardIndexReader.getFloat(i, readerContext)));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(Double.toString(forwardIndexReader.getDouble(i, readerContext)));
            }
            break;
          case STRING:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(forwardIndexReader.getString(i, readerContext));
            }
            break;
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              bloomFilterCreator.add(BytesUtils.toHexString(forwardIndexReader.getBytes(i, readerContext)));
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + columnMetadata.getDataType() + " for column: "
                + columnMetadata.getColumnName());
        }
        bloomFilterCreator.seal();
      } else {
        // MV
        switch (columnMetadata.getDataType()) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              int[] buffer = new int[columnMetadata.getMaxNumberOfMultiValues()];
              int length = forwardIndexReader.getIntMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(Integer.toString(buffer[j]));
              }
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              long[] buffer = new long[columnMetadata.getMaxNumberOfMultiValues()];
              int length = forwardIndexReader.getLongMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(Long.toString(buffer[j]));
              }
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              float[] buffer = new float[columnMetadata.getMaxNumberOfMultiValues()];
              int length = forwardIndexReader.getFloatMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(Float.toString(buffer[j]));
              }
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              double[] buffer = new double[columnMetadata.getMaxNumberOfMultiValues()];
              int length = forwardIndexReader.getDoubleMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(Double.toString(buffer[j]));
              }
            }
            break;
          case STRING:
            for (int i = 0; i < numDocs; i++) {
              String[] buffer = new String[columnMetadata.getMaxNumberOfMultiValues()];
              int length = forwardIndexReader.getStringMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(buffer[j]);
              }
            }
            break;
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              byte[][] buffer = new byte[columnMetadata.getMaxNumberOfMultiValues()][];
              int length = forwardIndexReader.getBytesMV(i, buffer, readerContext);
              for (int j = 0; j < length; j++) {
                bloomFilterCreator.add(BytesUtils.toHexString(buffer[j]));
              }
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + columnMetadata.getDataType() + " for column: "
                + columnMetadata.getColumnName());
        }
        bloomFilterCreator.seal();
      }
    }
  }

  private void createBloomFilterForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File bloomFilterFileInProgress = new File(indexDir, columnName + ".bloom.inprogress");
    File bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    if (!bloomFilterFileInProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(bloomFilterFileInProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove bloom filter file.
      FileUtils.deleteQuietly(bloomFilterFile);
    }

    if (!columnMetadata.hasDictionary()) {
      // Create a temporary forward index if it is disabled and does not exist
      columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);
    }

    // Create new bloom filter for the column.
    BloomFilterConfig bloomFilterConfig = _bloomFilterConfigs.get(columnName);
    LOGGER.info("Creating new bloom filter for segment: {}, column: {} with config: {}", segmentName, columnName,
        bloomFilterConfig);
    if (columnMetadata.hasDictionary()) {
      createAndSealBloomFilterForDictionaryColumn(indexDir, columnMetadata, bloomFilterConfig, segmentWriter);
    } else {
      createAndSealBloomFilterForNonDictionaryColumn(indexDir, columnMetadata, bloomFilterConfig, segmentWriter);
    }

    // For v3, write the generated bloom filter file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, bloomFilterFile, StandardIndexes.bloomFilter());
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(bloomFilterFileInProgress);
    LOGGER.info("Created bloom filter for segment: {}, column: {}", segmentName, columnName);
  }

  private Dictionary getDictionaryReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    DataType dataType = columnMetadata.getDataType();

    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        PinotDataBuffer buf = segmentWriter.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.dictionary());
        return DictionaryIndexType.read(buf, columnMetadata, DictionaryIndexConfig.DEFAULT);
      default:
        throw new IllegalStateException(
            "Unsupported data type: " + dataType + " for column: " + columnMetadata.getColumnName());
    }
  }
}
