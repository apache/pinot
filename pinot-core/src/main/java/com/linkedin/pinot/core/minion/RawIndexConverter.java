/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.minion;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.util.CrcUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>RawIndexConverter</code> class takes a segment and converts the dictionary-based indexes inside the segment
 * into raw indexes.
 * <ul>
 *   <li>
 *     If columns to convert are specified, check whether their dictionary-based indexes exist and convert them.
 *   </li>
 *   <li>
 *     If not specified, for each metric column, calculate the size of dictionary-based index and uncompressed raw
 *     index. If the size of raw index is smaller or equal to (size of dictionary-based index * CONVERSION_THRESHOLD),
 *     convert it.
 *   </li>
 * </ul>
 * <p>After the conversion, add "rawIndex" into the segment metadata "optimizations" field.
 */
public class RawIndexConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawIndexConverter.class);

  // Threshold for the ratio of uncompressed raw index size and dictionary-based index size to trigger conversion
  private static final int CONVERSION_THRESHOLD = 4;

  // BITS_PER_ELEMENT is not applicable for raw index
  private static final int BITS_PER_ELEMENT_FOR_RAW_INDEX = -1;

  private final IndexSegment _originalIndexSegment;
  private final SegmentMetadataImpl _originalSegmentMetadata;
  private final File _convertedIndexDir;
  private final PropertiesConfiguration _convertedProperties;
  private final String _columnsToConvert;

  /**
   * NOTE: original segment should be in V1 format.
   * TODO: support V3 format
   */
  public RawIndexConverter(@Nonnull File originalIndexDir, @Nonnull File convertedIndexDir,
      @Nullable String columnsToConvert) throws Exception {
    FileUtils.copyDirectory(originalIndexDir, convertedIndexDir);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    _originalIndexSegment = ColumnarSegmentLoader.load(originalIndexDir, indexLoadingConfig);
    _originalSegmentMetadata = (SegmentMetadataImpl) _originalIndexSegment.getSegmentMetadata();
    _convertedIndexDir = convertedIndexDir;
    _convertedProperties =
        new PropertiesConfiguration(new File(_convertedIndexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    _columnsToConvert = columnsToConvert;
  }

  public boolean convert() throws Exception {
    String segmentName = _originalSegmentMetadata.getName();
    String tableName = _originalSegmentMetadata.getTableName();
    LOGGER.info("Start converting segment: {} in table: {}", segmentName, tableName);

    List<FieldSpec> columnsToConvert = new ArrayList<>();
    Schema schema = _originalSegmentMetadata.getSchema();
    if (_columnsToConvert == null) {
      LOGGER.info("Columns to convert are not specified, check each metric column");
      for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
        if (_originalSegmentMetadata.hasDictionary(metricFieldSpec.getName()) && shouldConvertColumn(metricFieldSpec)) {
          columnsToConvert.add(metricFieldSpec);
        }
      }
    } else {
      LOGGER.info("Columns to convert: {}", _columnsToConvert);
      for (String columnToConvert : StringUtils.split(_columnsToConvert, ',')) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(columnToConvert);
        if (fieldSpec == null) {
          LOGGER.warn("Skip converting column: {} because is does not exist in the schema");
          continue;
        }
        if (!fieldSpec.isSingleValueField()) {
          LOGGER.warn("Skip converting column: {} because it's a multi-value column");
          continue;
        }
        if (!_originalSegmentMetadata.hasDictionary(columnToConvert)) {
          LOGGER.warn("Skip converting column: {} because its index is not dictionary-based");
          continue;
        }
        columnsToConvert.add(fieldSpec);
      }
    }

    if (columnsToConvert.isEmpty()) {
      LOGGER.info("No column converted for segment: {} in table: {}", segmentName, tableName);
      return false;
    } else {
      // Convert columns
      for (FieldSpec columnToConvert : columnsToConvert) {
        convertColumn(columnToConvert);
      }
      _convertedProperties.save();

      // Update creation metadata with new computed CRC and original segment creation time
      SegmentIndexCreationDriverImpl.persistCreationMeta(_convertedIndexDir,
          CrcUtils.forAllFilesInFolder(_convertedIndexDir).computeCrc(),
          _originalSegmentMetadata.getIndexCreationTime());

      LOGGER.info("{} columns converted for segment: {} in table: {}", columnsToConvert.size(), segmentName, tableName);
      return true;
    }
  }

  private boolean shouldConvertColumn(FieldSpec fieldSpec) {
    String columnName = fieldSpec.getName();
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    int numTotalDocs = _originalSegmentMetadata.getTotalDocs();
    ColumnMetadata columnMetadata = _originalSegmentMetadata.getColumnMetadataFor(columnName);

    int cardinality = columnMetadata.getCardinality();

    // In bits
    int lengthOfEachEntry;
    if (dataType.equals(FieldSpec.DataType.STRING)) {
      lengthOfEachEntry = columnMetadata.getStringColumnMaxLength() * Byte.SIZE;
    } else {
      lengthOfEachEntry = dataType.size() * Byte.SIZE;
    }
    long dictionaryBasedIndexSize =
        (long) numTotalDocs * columnMetadata.getBitsPerElement() + (long) cardinality * lengthOfEachEntry;
    long rawIndexSize = (long) numTotalDocs * lengthOfEachEntry;
    LOGGER.info(
        "For column: {}, size of dictionary based index: {} bits, size of raw index (without compression): {} bits",
        columnName, dictionaryBasedIndexSize, rawIndexSize);

    return rawIndexSize <= dictionaryBasedIndexSize * CONVERSION_THRESHOLD;
  }

  private void convertColumn(FieldSpec fieldSpec) throws Exception {
    String columnName = fieldSpec.getName();
    LOGGER.info("Converting column: {}", columnName);

    // Delete dictionary and existing indexes
    FileUtils.deleteQuietly(new File(_convertedIndexDir, columnName + V1Constants.Dict.FILE_EXTENSION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION));

    // Create the raw index
    DataSource dataSource = _originalIndexSegment.getDataSource(columnName);
    Dictionary dictionary = dataSource.getDictionary();
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    int lengthOfLongestEntry = _originalSegmentMetadata.getColumnMetadataFor(columnName).getStringColumnMaxLength();
    try (SingleValueRawIndexCreator rawIndexCreator = SegmentColumnarIndexCreator.getRawIndexCreatorForColumn(
        _convertedIndexDir, ChunkCompressorFactory.CompressionType.SNAPPY, columnName, dataType,
        _originalSegmentMetadata.getTotalDocs(), lengthOfLongestEntry)) {
      BlockSingleValIterator iterator = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();
      int docId = 0;
      while (iterator.hasNext()) {
        int dictId = iterator.nextIntVal();
        rawIndexCreator.index(docId++, dictionary.get(dictId));
      }
    }

    // Update the segment metadata
    _convertedProperties.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(columnName, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
    _convertedProperties.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(columnName, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT),
        BITS_PER_ELEMENT_FOR_RAW_INDEX);
  }
}
