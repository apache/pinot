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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.inverted.InvertedIndexType;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;


/**
 * Helper classed used by the {@link SegmentPreProcessor} to generate the forward index from inverted index and
 * dictionary when the forward index is enabled for columns where it was previously disabled. This is also invoked by
 * the {@link IndexHandler} code in scenarios where the forward index needs to be temporarily created to generate other
 * indexes for the given column. In such cases the forward index will be cleaned up after the {@link IndexHandler} code
 * completes.
 *
 * For multi-value columns the following invariants cannot be maintained:
 * - Ordering of elements within a given multi-value row. This will always be a limitation.
 *
 * TODO: Currently for multi-value columns generating the forward index can lead to a data loss as frequency information
 *       is not available for repeats within a given row. This needs to be addressed by tracking the frequency data
 *       as part of an on-disk structure when forward index is disabled for a column.
 *
 * TODO (index-spi): Rename this class, as it is not an implementation of
 * {@link org.apache.pinot.segment.spi.index.IndexCreator}.
 */
public class InvertedIndexAndDictionaryBasedForwardIndexCreator implements AutoCloseable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(InvertedIndexAndDictionaryBasedForwardIndexCreator.class);

  // Use MMapBuffer if the value buffer size is larger than 2G
  private static final int NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER = 500_000_000;

  private static final String FORWARD_INDEX_VALUE_BUFFER_SUFFIX = ".fwd.idx.val.buf";
  private static final String FORWARD_INDEX_LENGTH_BUFFER_SUFFIX = ".fwd.idx.len.buf";
  private static final String FORWARD_INDEX_MAX_SIZE_BUFFER_SUFFIX = ".fwd.idx.maxsize.buf";

  private final SegmentDirectory _segmentDirectory;
  private final SegmentDirectory.Writer _segmentWriter;
  private final TableConfig _tableConfig;
  private final String _columnName;
  /// `true` when the post-rebuild config keeps a standalone dictionary file for the column. Set to `false` only
  /// when the dictionary is being dropped as part of the rebuild — independent of whether the new forward index
  /// is dict-encoded or raw, since a raw forward index can also share an existing dictionary.
  private final boolean _keepDictionary;
  private final ForwardIndexConfig _forwardIndexConfig;
  private final boolean _isTemporaryForwardIndex;

  // Metadata
  private final ColumnMetadata _columnMetadata;
  private final boolean _singleValue;
  private final int _cardinality;
  private final int _numDocs;
  private final int _maxNumberOfMultiValues;
  private final int _totalNumberOfEntries;
  private final boolean _useMMapBuffer;

  // Files and temporary buffers
  private final File _forwardIndexFile;
  private final File _forwardIndexValueBufferFile;
  private final File _forwardIndexLengthBufferFile;
  private final File _forwardIndexMaxSizeBufferFile;

  // SegmentMetadata may need to be updated
  private SegmentMetadata _segmentMetadata;

  // Forward index buffers (to store the dictId at the correct docId)
  private PinotDataBuffer _forwardIndexValueBuffer;
  // For multi-valued column only because each docId can have multiple dictIds
  private PinotDataBuffer _forwardIndexLengthBuffer;
  private int _nextValueId;
  // For multi-valued column only to track max row size
  private PinotDataBuffer _forwardIndexMaxSizeBuffer;

  public InvertedIndexAndDictionaryBasedForwardIndexCreator(SegmentDirectory segmentDirectory,
      SegmentDirectory.Writer segmentWriter, TableConfig tableConfig, String columnName,
      FieldIndexConfigs fieldIndexConfigs, boolean isTemporaryForwardIndex)
      throws IOException {
    _segmentDirectory = segmentDirectory;
    _segmentWriter = segmentWriter;
    _tableConfig = tableConfig;
    _columnName = columnName;
    _keepDictionary = fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled();
    _forwardIndexConfig = fieldIndexConfigs.getConfig(StandardIndexes.forward());
    _isTemporaryForwardIndex = isTemporaryForwardIndex;

    _segmentMetadata = segmentDirectory.getSegmentMetadata();
    _columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    _singleValue = _columnMetadata.isSingleValue();
    _numDocs = _columnMetadata.getTotalDocs();
    _cardinality = _columnMetadata.getCardinality();
    assert _numDocs > 0 && _cardinality > 0;
    _totalNumberOfEntries = _columnMetadata.getTotalNumberOfEntries();
    _maxNumberOfMultiValues = _columnMetadata.getMaxNumberOfMultiValues();
    _useMMapBuffer = _totalNumberOfEntries > NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER;

    // Sorted columns should never need recreation of the forward index as the forwardIndexDisabled flag is treated as
    // a no-op for sorted columns
    File indexDir = _segmentMetadata.getIndexDir();
    String fileExtension;
    if (_forwardIndexConfig.getEncodingType() == EncodingType.DICTIONARY) {
      fileExtension = _singleValue ? V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION
          : V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      fileExtension = _singleValue ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
          : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
    _forwardIndexFile = new File(indexDir, columnName + fileExtension);
    _forwardIndexValueBufferFile = new File(indexDir, columnName + FORWARD_INDEX_VALUE_BUFFER_SUFFIX);
    _forwardIndexLengthBufferFile = new File(indexDir, columnName + FORWARD_INDEX_LENGTH_BUFFER_SUFFIX);
    _forwardIndexMaxSizeBufferFile = new File(indexDir, columnName + FORWARD_INDEX_MAX_SIZE_BUFFER_SUFFIX);

    // Create the temporary buffers needed
    try {
      _forwardIndexValueBuffer =
          createTempBuffer((long) _totalNumberOfEntries * Integer.BYTES, _forwardIndexValueBufferFile);
      if (!_singleValue) {
        _forwardIndexLengthBuffer = createTempBuffer((long) _numDocs * Integer.BYTES, _forwardIndexLengthBufferFile);
        for (int i = 0; i < _numDocs; i++) {
          // We need to clear the forward index length buffer because we rely on the initial value of 0, and keep
          // updating the value instead of directly setting the value
          _forwardIndexLengthBuffer.putInt((long) i * Integer.BYTES, 0);
        }
        _forwardIndexMaxSizeBuffer = createTempBuffer((long) _numDocs * Integer.BYTES, _forwardIndexMaxSizeBufferFile);
        for (int i = 0; i < _numDocs; i++) {
          // We need to clear the forward index max size buffer because we rely on the initial value of 0, and keep
          // updating the value instead of directly setting the value
          _forwardIndexMaxSizeBuffer.putInt((long) i * Integer.BYTES, 0);
        }
      }
    } catch (Exception e) {
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      destroyBuffer(_forwardIndexMaxSizeBuffer, _forwardIndexMaxSizeBufferFile);
      throw new IOException("Couldn't create temp buffers to construct forward index", e);
    }
  }

  public void regenerateForwardIndex()
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    File inProgress = new File(indexDir, _columnName + ".fwd.inprogress");

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index if exists.
      FileUtils.deleteQuietly(_forwardIndexFile);
    }

    // Create new forward index for the column.
    LOGGER.info("Creating a new forward index for segment: {}, column: {}, isTemporary: {}", segmentName, _columnName,
        _isTemporaryForwardIndex);

    Map<String, String> metadataProperties;
    if (_singleValue) {
      metadataProperties = createForwardIndexForSVColumn();
    } else {
      metadataProperties = createForwardIndexForMVColumn();
    }

    LoaderUtils.writeIndexToV3Format(_segmentWriter, _columnName, _forwardIndexFile, StandardIndexes.forward());

    try {
      // Update the metadata even for temporary forward index as other IndexHandlers may rely on the updated metadata
      // to construct their indexes based on the forward index. For temporary forward index, this metadata will not
      // be reset on forward index deletion for two reasons: a) for SV columns only dictionary related metadata is
      // modified and for temporary forward index scenarios dictionary must be present and cannot be changed, b) for
      // MV columns, in addition to dictionary related metadata, MAX_MULTI_VALUE_ELEMENTS and TOTAL_NUMBER_OF_ENTRIES
      // may be modified which can be left behind in the modified state even on forward index deletion.
      LOGGER.info("Created forward index from inverted index and dictionary. Updating metadata properties for "
              + "segment: {}, column: {}, property list: {}, is temporary: {}", segmentName, _columnName,
          metadataProperties, _isTemporaryForwardIndex);
      if (!metadataProperties.isEmpty()) {
        _segmentMetadata = SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);
      }
    } catch (Exception e) {
      throw new IOException(
          String.format("Failed to update metadata properties for segment: %s, column: %s", segmentName, _columnName),
          e);
    }

    if (!_isTemporaryForwardIndex) {
      // Only cleanup the other indexes if the forward index to be created is permanent. If the forward index is
      // temporary, it is meant to be used only for construction of other indexes and will be deleted once all the
      // IndexHandlers have completed.
      if (!_keepDictionary) {
        LOGGER.info("Clean up indexes no longer needed or which need to be rewritten for segment: {}, column: {}",
            segmentName, _columnName);
        // Delete the dictionary
        _segmentWriter.removeIndex(_columnName, StandardIndexes.dictionary());

        // We remove indexes that have to be rewritten when a dictEnabled is toggled. Note that the respective index
        // handler will take care of recreating the index.
        ForwardIndexHandler.removeDictRelatedIndexes(_columnName, _segmentWriter);
      }
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created a new forward index for segment: {}, column: {}, isTemporary: {}", segmentName, _columnName,
        _isTemporaryForwardIndex);
  }

  private Map<String, String> createForwardIndexForSVColumn()
      throws IOException {
    try (BitmapInvertedIndexReader invertedIndexReader =
        (BitmapInvertedIndexReader) InvertedIndexType.ReaderFactory
            .INSTANCE.createSkippingForward(_segmentWriter, _columnMetadata);
        Dictionary dictionary = DictionaryIndexType.read(_segmentWriter, _columnMetadata)) {
      // Construct the forward index in the values buffer. For var-length columns, also gather per-element stats
      // (lengthOfShortest/Longest, isAscii for STRING) inline when the source segment is missing them, so the
      // backfill happens without a second dictionary scan.
      DataType storedType = _columnMetadata.getStoredType();
      boolean backfillStats =
          !storedType.isFixedWidth() && _columnMetadata.getLengthOfShortestElement() < 0;
      int lengthOfShortestElement = Integer.MAX_VALUE;
      int lengthOfLongestElement = 0;
      boolean isAscii = storedType == DataType.STRING;
      for (int dictId = 0; dictId < _cardinality; dictId++) {
        ImmutableRoaringBitmap docIdsBitmap = invertedIndexReader.getDocIds(dictId);
        int finalDictId = dictId;
        docIdsBitmap.stream().forEach(docId -> putInt(_forwardIndexValueBuffer, docId, finalDictId));
        if (backfillStats) {
          int valueSize = dictionary.getValueSize(dictId);
          lengthOfShortestElement = Math.min(lengthOfShortestElement, valueSize);
          lengthOfLongestElement = Math.max(lengthOfLongestElement, valueSize);
          if (isAscii) {
            isAscii = valueSize == dictionary.getStringValue(dictId).length();
          }
        }
      }

      IndexCreationContext.Builder builder =
          new IndexCreationContext.Builder(_segmentMetadata.getIndexDir(), _tableConfig, _columnMetadata);
      if (backfillStats) {
        builder.withLengthOfShortestElement(lengthOfShortestElement);
        builder.withLengthOfLongestElement(lengthOfLongestElement);
        builder.withAscii(isAscii);
      }

      // NOTE: this method closes buffers and removes files
      writeToForwardIndex(dictionary, builder.build());

      // Setup and return the metadata properties to update
      Map<String, String> metadataProperties = new HashMap<>();
      metadataProperties.put(getKeyFor(_columnName, FORWARD_INDEX_ENCODING),
          _forwardIndexConfig.getEncodingType().name());
      if (!_keepDictionary) {
        metadataProperties.put(getKeyFor(_columnName, HAS_DICTIONARY), String.valueOf(false));
        metadataProperties.put(getKeyFor(_columnName, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
        // TODO: See https://github.com/apache/pinot/pull/16921 for details
        // TODO: Remove the property after 1.6.0 release
        // metadataProperties.put(getKeyFor(_columnName, BITS_PER_ELEMENT), null);
      }
      if (backfillStats) {
        metadataProperties.put(getKeyFor(_columnName, LENGTH_OF_SHORTEST_ELEMENT),
            String.valueOf(lengthOfShortestElement));
        metadataProperties.put(getKeyFor(_columnName, LENGTH_OF_LONGEST_ELEMENT),
            String.valueOf(lengthOfLongestElement));
        if (storedType == DataType.STRING) {
          metadataProperties.put(getKeyFor(_columnName, IS_ASCII), String.valueOf(isAscii));
        }
      }
      return metadataProperties;
    }
  }

  private Map<String, String> createForwardIndexForMVColumn()
      throws IOException {
    try (BitmapInvertedIndexReader invertedIndexReader =
        (BitmapInvertedIndexReader) InvertedIndexType.ReaderFactory.INSTANCE
            .createSkippingForward(_segmentWriter, _columnMetadata);
        Dictionary dictionary = DictionaryIndexType.read(_segmentWriter, _columnMetadata)) {
      // Construct the forward index length buffer and create the inverted index values and length buffers
      int maxNumberOfMultiValues = 0;
      for (int dictId = 0; dictId < _cardinality; dictId++) {
        ImmutableRoaringBitmap docIdsBitmap = invertedIndexReader.getDocIds(dictId);
        PeekableIntIterator intIterator = docIdsBitmap.getIntIterator();
        while (intIterator.hasNext()) {
          int docId = intIterator.next();
          int numMultiValues = getInt(_forwardIndexLengthBuffer, docId) + 1;
          putInt(_forwardIndexLengthBuffer, docId, numMultiValues);
          maxNumberOfMultiValues = Math.max(maxNumberOfMultiValues, numMultiValues);
          _nextValueId++;
        }
      }

      if (_nextValueId < _totalNumberOfEntries) {
        LOGGER.warn("Total number of entries: {} less than expected total number of entries: {}, multi-value column: "
                + "{} duplicates detected, duplicate entries within each row lost! Expected maxNumberOfMultiValues: "
                + "{}, actual maxNumberOfMultiValues: {}", _nextValueId, _totalNumberOfEntries, _columnName,
            _maxNumberOfMultiValues, maxNumberOfMultiValues);
      } else {
        Preconditions.checkState(_nextValueId == _totalNumberOfEntries,
            String.format("Number of entries found %d cannot be higher than expected total number of entries: %d for "
                    + "column: %s", _nextValueId, _totalNumberOfEntries, _columnName));
        Preconditions.checkState(maxNumberOfMultiValues == _maxNumberOfMultiValues,
            String.format("Actual maxNumberOfMultiValues: %d doesn't match expected maxNumberOfMultiValues: %d for "
                + "column %s", maxNumberOfMultiValues, _maxNumberOfMultiValues, _columnName));
      }

      // Calculate value index for each docId in the forward index value buffer
      // Re-use forward index length buffer to store the value index for each docId, where value index is the index in
      // the forward index value buffer where we should put next dictId for the docId
      int forwardValueIndex = 0;
      for (int docId = 0; docId < _numDocs; docId++) {
        int length = getInt(_forwardIndexLengthBuffer, docId);
        putInt(_forwardIndexLengthBuffer, docId, forwardValueIndex);
        forwardValueIndex += length;
      }

      // Construct the forward index values buffer from the inverted index using the length buffer for index tracking.
      // For var-length columns, also tracks per-element stats (lengthOfShortest/Longest, isAscii for STRING) when
      // the source segment is missing them, so we can backfill those metadata keys without a second dictionary scan.
      DataType storedType = _columnMetadata.getStoredType();
      boolean isFixedWidth = storedType.isFixedWidth();
      int fixedSize = isFixedWidth ? storedType.size() : 0;
      int maxRowLengthInBytes = isFixedWidth ? maxNumberOfMultiValues * fixedSize : 0;
      boolean backfillStats = !isFixedWidth && _columnMetadata.getLengthOfShortestElement() < 0;
      int lengthOfShortestElement = Integer.MAX_VALUE;
      int lengthOfLongestElement = 0;
      boolean isAscii = storedType == DataType.STRING;
      for (int dictId = 0; dictId < _cardinality; dictId++) {
        ImmutableRoaringBitmap docIdsBitmap = invertedIndexReader.getDocIds(dictId);
        PeekableIntIterator intIterator = docIdsBitmap.getIntIterator();
        int valueSize = isFixedWidth ? fixedSize : dictionary.getValueSize(dictId);
        if (backfillStats) {
          lengthOfShortestElement = Math.min(lengthOfShortestElement, valueSize);
          lengthOfLongestElement = Math.max(lengthOfLongestElement, valueSize);
          if (isAscii) {
            isAscii = valueSize == dictionary.getStringValue(dictId).length();
          }
        }
        while (intIterator.hasNext()) {
          int docId = intIterator.next();
          int index = getInt(_forwardIndexLengthBuffer, docId);
          putInt(_forwardIndexValueBuffer, index, dictId);
          putInt(_forwardIndexLengthBuffer, docId, index + 1);
          if (!isFixedWidth) {
            int currentRowLength = getInt(_forwardIndexMaxSizeBuffer, docId);
            int newRowLength = currentRowLength + valueSize;
            putInt(_forwardIndexMaxSizeBuffer, docId, newRowLength);
            maxRowLengthInBytes = Math.max(maxRowLengthInBytes, newRowLength);
          }
        }
      }

      // When the duplicate-detection branch above fires, `_nextValueId` and `maxNumberOfMultiValues` are the new
      // post-dedup values and differ from the source metadata; override so the forward index creator sees the correct
      // sizes (consistent with the persisted metadata properties below).
      File indexDir = _segmentMetadata.getIndexDir();
      IndexCreationContext.Builder builder = new IndexCreationContext.Builder(indexDir, _tableConfig, _columnMetadata)
          .withTotalNumberOfEntries(_nextValueId)
          .withMaxNumberOfMultiValues(maxNumberOfMultiValues)
          .withMaxRowLengthInBytes(maxRowLengthInBytes);
      if (backfillStats) {
        builder.withLengthOfShortestElement(lengthOfShortestElement);
        builder.withLengthOfLongestElement(lengthOfLongestElement);
        builder.withAscii(isAscii);
      }

      writeToForwardIndex(dictionary, builder.build());

      // Setup and return the metadata properties to update
      Map<String, String> metadataProperties = new HashMap<>();
      metadataProperties.put(getKeyFor(_columnName, FORWARD_INDEX_ENCODING),
          _forwardIndexConfig.getEncodingType().name());
      if (!_keepDictionary) {
        metadataProperties.put(getKeyFor(_columnName, HAS_DICTIONARY), String.valueOf(false));
        metadataProperties.put(getKeyFor(_columnName, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
        // TODO: See https://github.com/apache/pinot/pull/16921 for details
        // TODO: Remove the property after 1.6.0 release
        // metadataProperties.put(getKeyFor(_columnName, BITS_PER_ELEMENT), null);
      }
      metadataProperties.put(getKeyFor(_columnName, TOTAL_NUMBER_OF_ENTRIES), String.valueOf(_nextValueId));
      metadataProperties.put(getKeyFor(_columnName, MAX_MULTI_VALUE_ELEMENTS), String.valueOf(maxNumberOfMultiValues));
      if (!isFixedWidth) {
        metadataProperties.put(getKeyFor(_columnName, MAX_ROW_LENGTH_IN_BYTES), String.valueOf(maxRowLengthInBytes));
        if (backfillStats) {
          metadataProperties.put(getKeyFor(_columnName, LENGTH_OF_SHORTEST_ELEMENT),
              String.valueOf(lengthOfShortestElement));
          metadataProperties.put(getKeyFor(_columnName, LENGTH_OF_LONGEST_ELEMENT),
              String.valueOf(lengthOfLongestElement));
          if (storedType == DataType.STRING) {
            metadataProperties.put(getKeyFor(_columnName, IS_ASCII), String.valueOf(isAscii));
          }
        }
      }
      return metadataProperties;
    }
  }

  private void writeToForwardIndex(Dictionary dictionary, IndexCreationContext context)
      throws IOException {
    try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, _forwardIndexConfig)) {
      if (creator.isDictionaryEncoded()) {
        if (_singleValue) {
          for (int docId = 0; docId < _numDocs; docId++) {
            creator.putDictId(getInt(_forwardIndexValueBuffer, docId));
          }
        } else {
          int startIdx = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            int endIdx = getInt(_forwardIndexLengthBuffer, docId);
            int[] values = new int[endIdx - startIdx];
            int valuesIdx = 0;
            for (int i = startIdx; i < endIdx; i++) {
              values[valuesIdx++] = getInt(_forwardIndexValueBuffer, i);
            }
            creator.putDictIdMV(values);
            startIdx = endIdx;
          }
        }
      } else {
        switch (creator.getValueType()) {
          case INT:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putInt(dictionary.getIntValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                int[] values = new int[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getIntValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putIntMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case LONG:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putLong(dictionary.getLongValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                long[] values = new long[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getLongValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putLongMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case FLOAT:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putFloat(dictionary.getFloatValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                float[] values = new float[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getFloatValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putFloatMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case DOUBLE:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putDouble(dictionary.getDoubleValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                double[] values = new double[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getDoubleValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putDoubleMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case BIG_DECIMAL:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putBigDecimal(dictionary.getBigDecimalValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                BigDecimal[] values = new BigDecimal[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getBigDecimalValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putBigDecimalMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case STRING:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putString(dictionary.getStringValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                String[] values = new String[endIdx - startIdx];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getStringValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putStringMV(values);
                startIdx = endIdx;
              }
            }
            break;
          case BYTES:
            if (_singleValue) {
              for (int docId = 0; docId < _numDocs; docId++) {
                creator.putBytes(dictionary.getBytesValue(getInt(_forwardIndexValueBuffer, docId)));
              }
            } else {
              int startIdx = 0;
              for (int docId = 0; docId < _numDocs; docId++) {
                int endIdx = getInt(_forwardIndexLengthBuffer, docId);
                byte[][] values = new byte[endIdx - startIdx][];
                int valuesIdx = 0;
                for (int i = startIdx; i < endIdx; i++) {
                  values[valuesIdx++] = dictionary.getBytesValue(getInt(_forwardIndexValueBuffer, i));
                }
                creator.putBytesMV(values);
                startIdx = endIdx;
              }
            }
            break;
          default:
            throw new IllegalStateException("Invalid type" + creator.getValueType() + " cannot create forward index");
        }
      }
    } catch (Exception e) {
      throw new IOException(String.format(
          "Cannot create the forward index from inverted index for column %s", _columnName), e);
    } finally {
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      destroyBuffer(_forwardIndexMaxSizeBuffer, _forwardIndexMaxSizeBufferFile);
    }
  }

  private static void putInt(PinotDataBuffer buffer, long index, int value) {
    buffer.putInt(index << 2, value);
  }

  private static int getInt(PinotDataBuffer buffer, long index) {
    return buffer.getInt(index << 2);
  }

  private PinotDataBuffer createTempBuffer(long size, File mmapFile)
      throws IOException {
    if (_useMMapBuffer) {
      return PinotDataBuffer.mapFile(mmapFile, false, 0, size, PinotDataBuffer.NATIVE_ORDER,
          "InvertedIndexAndDictionaryBasedForwardIndexCreator: temp mmapped buffer for " + mmapFile.getName());
    } else {
      return PinotDataBuffer.allocateDirect(size, PinotDataBuffer.NATIVE_ORDER,
          "InvertedIndexAndDictionaryBasedForwardIndexCreator: temp direct buffer for " + mmapFile.getName());
    }
  }

  private void destroyBuffer(PinotDataBuffer buffer, File mmapFile)
      throws IOException {
    if (buffer != null) {
      buffer.close();
    }
    if (mmapFile.exists()) {
      FileUtils.forceDelete(mmapFile);
    }
  }

  @Override
  public void close()
      throws Exception {
    destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
    destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
    destroyBuffer(_forwardIndexMaxSizeBuffer, _forwardIndexMaxSizeBufferFile);
  }
}
