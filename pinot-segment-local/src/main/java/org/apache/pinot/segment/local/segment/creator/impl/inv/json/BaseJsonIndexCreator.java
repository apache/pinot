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
package org.apache.pinot.segment.local.segment.creator.impl.inv.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexType;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexUtils;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.utils.MetricUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.Container;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import static java.nio.charset.StandardCharsets.UTF_8;


/// Base implementation of the json index creator.
///
/// Header format:
///
/// - Version (`int`)
/// - Max value length (`int`)
/// - Dictionary file length (`long`)
/// - Inverted index file length (`long`)
/// - Doc id mapping file length (`long`)
///
/// V2 keeps the doc-id mapping and can append an optional direct-doc-id sidecar after it. A non-zero sidecar stores a
/// direct-doc-id dictionary and inverted index for scalar paths. Older V2 readers ignore the trailing sidecar and keep
/// using the flattened-doc-id dictionary.
///
/// V3 is used only when flattened doc ids are identical to real doc ids. It stores the same dictionary and inverted
/// index encoding as V2, omits the identity doc-id mapping, and appends a zero-length sidecar marker.
public abstract class BaseJsonIndexCreator implements JsonIndexCreator {
  // NOTE: V1 is deprecated because it does not support top-level value, top-level array and nested array
  public static final int VERSION_1 = 1;
  public static final int VERSION_2 = 2;
  public static final int VERSION_3 = 3;
  public static final int HEADER_LENGTH = 32;

  static final String TEMP_DIR_SUFFIX = ".json.idx.tmp";
  static final String DICTIONARY_FILE_NAME = "dictionary.buf";
  static final String INVERTED_INDEX_FILE_NAME = "inverted.index.buf";
  static final String DIRECT_DOC_ID_DICTIONARY_FILE_NAME = "direct_doc_id_dictionary.buf";
  static final String DIRECT_DOC_ID_INVERTED_INDEX_FILE_NAME = "direct_doc_id_inverted.index.buf";
  public static final int DIRECT_DOC_ID_INDEX_HEADER_LENGTH = Long.BYTES * 2;

  final String _tableNameWithType;
  final boolean _continueOnError;
  final JsonIndexConfig _jsonIndexConfig;
  final File _indexFile;
  final File _tempDir;
  final File _dictionaryFile;
  final File _invertedIndexFile;
  final File _directDocIdDictionaryFile;
  final File _directDocIdInvertedIndexFile;
  final IntList _numFlattenedRecordsList = new IntArrayList();
  final Map<String, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();

  int _nextFlattenedDocId;
  int _maxValueLength;
  boolean _docIdMappingIdentity = true;

  BaseJsonIndexCreator(File indexDir, String columnName, String tableNameWithType, boolean continueOnError,
      JsonIndexConfig jsonIndexConfig)
      throws IOException {
    _tableNameWithType = tableNameWithType;
    _continueOnError = continueOnError;
    _jsonIndexConfig = jsonIndexConfig;
    _indexFile = new File(indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, columnName + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _dictionaryFile = new File(_tempDir, DICTIONARY_FILE_NAME);
    _invertedIndexFile = new File(_tempDir, INVERTED_INDEX_FILE_NAME);
    _directDocIdDictionaryFile = new File(_tempDir, DIRECT_DOC_ID_DICTIONARY_FILE_NAME);
    _directDocIdInvertedIndexFile = new File(_tempDir, DIRECT_DOC_ID_INVERTED_INDEX_FILE_NAME);
  }

  @Override
  public void add(String jsonString)
      throws IOException {
    List<Map<String, String>> flattenedRecord;
    try {
      flattenedRecord = JsonUtils.flatten(jsonString, _jsonIndexConfig);
      if (flattenedRecord == JsonUtils.SKIPPED_FLATTENED_RECORD) {
        // The default SKIPPED_FLATTENED_RECORD was returned, this can only happen if the original record could not be
        // flattened, update the metric
        MetricUtils.updateIndexingErrorMetric(_tableNameWithType, JsonIndexType.INDEX_DISPLAY_NAME);
      }
    } catch (Exception e) {
      if (_continueOnError) {
        // Caught exception while trying to add, update metric and add a default SKIPPED_FLATTENED_RECORD
        // This check is needed in the case where `_jsonIndexConfig.getSkipInvalidJson()` is false,
        // but _continueOnError is true
        MetricUtils.updateIndexingErrorMetric(_tableNameWithType, JsonIndexType.INDEX_DISPLAY_NAME);
        flattenedRecord = JsonUtils.SKIPPED_FLATTENED_RECORD;
      } else {
        throw e;
      }
    }
    addFlattenedRecords(flattenedRecord);
  }

  @Override
  public void add(Map value)
      throws IOException {
    String valueToAdd;
    try {
      // TODO: Avoid this ser/de from map -> string -> json node
      valueToAdd = JsonUtils.objectToString(value);
    } catch (JsonProcessingException e) {
      if (_jsonIndexConfig.getSkipInvalidJson() || _continueOnError) {
        // Caught exception while trying to add, update metric and add a default SKIPPED_FLATTENED_RECORD
        MetricUtils.updateIndexingErrorMetric(_tableNameWithType, JsonIndexType.INDEX_DISPLAY_NAME);
        addFlattenedRecords(JsonUtils.SKIPPED_FLATTENED_RECORD);
        return;
      } else {
        throw e;
      }
    }
    add(valueToAdd);
  }

  /**
   * Adds the flattened records for the next document.
   */
  void addFlattenedRecords(List<Map<String, String>> records)
      throws IOException {
    int numRecords = records.size();
    Preconditions.checkState(_nextFlattenedDocId + numRecords >= 0, "Got more than %s flattened records",
        Integer.MAX_VALUE);
    if (numRecords != 1 || _nextFlattenedDocId != _numFlattenedRecordsList.size()) {
      _docIdMappingIdentity = false;
    }
    _numFlattenedRecordsList.add(numRecords);
    for (Map<String, String> record : records) {
      for (Map.Entry<String, String> entry : record.entrySet()) {
        // Put both key and key-value into the posting list. Key is useful for checking if a key exists in the json.
        String key = entry.getKey();
        addToPostingList(key);
        String keyValue = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + entry.getValue();
        addToPostingList(keyValue);
      }
      _nextFlattenedDocId++;
    }
  }

  /**
   * Adds the given value to the posting list.
   */
  void addToPostingList(String value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(value, bitmapWriter);
    }
    bitmapWriter.add(_nextFlattenedDocId);
  }

  /**
   * Generates the index file based on _maxValueLength, _dictionaryFile, _invertedIndexFile, _numFlattenedRecordsList,
   * _nextFlattenedDocId.
   */
  void generateIndexFile()
      throws IOException {
    boolean hasDirectDocIdIndex = generateDirectDocIdIndexFiles();
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    boolean omitDocIdMapping = _docIdMappingIdentity;
    headerBuffer.putInt(omitDocIdMapping ? VERSION_3 : VERSION_2);
    headerBuffer.putInt(_maxValueLength);
    long dictionaryFileLength = _dictionaryFile.length();
    long invertedIndexFileLength = _invertedIndexFile.length();
    long docIdMappingFileLength = omitDocIdMapping ? 0 : (long) _nextFlattenedDocId << 2;
    boolean hasDirectDocIdIndexHeader = omitDocIdMapping || hasDirectDocIdIndex;
    long directDocIdDictionaryFileLength = hasDirectDocIdIndex ? _directDocIdDictionaryFile.length() : 0;
    long directDocIdInvertedIndexFileLength = hasDirectDocIdIndex ? _directDocIdInvertedIndexFile.length() : 0;
    headerBuffer.putLong(dictionaryFileLength);
    headerBuffer.putLong(invertedIndexFileLength);
    headerBuffer.putLong(docIdMappingFileLength);
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel dictionaryFileChannel = new RandomAccessFile(_dictionaryFile, "r").getChannel();
        FileChannel invertedIndexFileChannel = new RandomAccessFile(_invertedIndexFile, "r").getChannel();
        FileChannel directDocIdDictionaryFileChannel = hasDirectDocIdIndex
            ? new RandomAccessFile(_directDocIdDictionaryFile, "r").getChannel() : null;
        FileChannel directDocIdInvertedIndexFileChannel = hasDirectDocIdIndex
            ? new RandomAccessFile(_directDocIdInvertedIndexFile, "r").getChannel() : null) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(dictionaryFileChannel, 0, dictionaryFileLength,
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(invertedIndexFileChannel, 0, invertedIndexFileLength,
          indexFileChannel);

      // Write the doc id mapping to the index file.
      long docIdMappingStartOffset = indexFileChannel.position();
      if (!omitDocIdMapping) {
        ByteBuffer docIdMappingBuffer =
            indexFileChannel.map(FileChannel.MapMode.READ_WRITE, docIdMappingStartOffset, docIdMappingFileLength)
                .order(ByteOrder.LITTLE_ENDIAN);
        int numDocs = _numFlattenedRecordsList.size();
        for (int i = 0; i < numDocs; i++) {
          int numRecords = _numFlattenedRecordsList.getInt(i);
          for (int j = 0; j < numRecords; j++) {
            docIdMappingBuffer.putInt(i);
          }
        }
        if (CleanerUtil.UNMAP_SUPPORTED) {
          CleanerUtil.BufferCleaner cleaner = CleanerUtil.getCleaner();
          cleaner.freeBuffer(docIdMappingBuffer);
        }
      }
      indexFileChannel.position(docIdMappingStartOffset + docIdMappingFileLength);
      if (hasDirectDocIdIndexHeader) {
        ByteBuffer directDocIdHeaderBuffer = ByteBuffer.allocate(DIRECT_DOC_ID_INDEX_HEADER_LENGTH);
        directDocIdHeaderBuffer.putLong(directDocIdDictionaryFileLength);
        directDocIdHeaderBuffer.putLong(directDocIdInvertedIndexFileLength);
        directDocIdHeaderBuffer.position(0);
        indexFileChannel.write(directDocIdHeaderBuffer);
        if (hasDirectDocIdIndex) {
          org.apache.pinot.common.utils.FileUtils.transferBytes(directDocIdDictionaryFileChannel, 0,
              directDocIdDictionaryFileLength, indexFileChannel);
          org.apache.pinot.common.utils.FileUtils.transferBytes(directDocIdInvertedIndexFileChannel, 0,
              directDocIdInvertedIndexFileLength, indexFileChannel);
        }
      }
      indexFileChannel.truncate(indexFileChannel.position());
      indexFileChannel.force(true);
    }
  }

  private boolean generateDirectDocIdIndexFiles()
      throws IOException {
    if (_docIdMappingIdentity) {
      return false;
    }

    try (PinotDataBuffer dictionaryBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(_dictionaryFile);
        PinotDataBuffer invertedIndexBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(_invertedIndexFile)) {
      StringDictionary dictionary = new StringDictionary(dictionaryBuffer, 0, _maxValueLength);
      int numDirectDocIdPostingLists = countDirectDocIdPostingLists(dictionary);
      if (numDirectDocIdPostingLists == 0) {
        return false;
      }

      BitmapInvertedIndexReader invertedIndex =
          new BitmapInvertedIndexReader(invertedIndexBuffer, dictionary.length());
      int[] flattenedDocIdUpperBounds = buildFlattenedDocIdUpperBounds();
      try (VarLengthValueWriter dictionaryWriter =
          new VarLengthValueWriter(_directDocIdDictionaryFile, numDirectDocIdPostingLists);
          BitmapInvertedIndexWriter invertedIndexWriter =
              new BitmapInvertedIndexWriter(_directDocIdInvertedIndexFile, numDirectDocIdPostingLists)) {
        byte[] dictBuffer = dictionary.getBuffer();
        for (int dictId = 0, length = dictionary.length(); dictId < length; dictId++) {
          String value = dictionary.getStringValue(dictId, dictBuffer);
          if (JsonIndexUtils.isDirectDocIdIndexEligibleValue(value)) {
            byte[] valueBytes = value.getBytes(UTF_8);
            dictionaryWriter.add(valueBytes);
            RoaringBitmap docIds = new RoaringBitmap();
            invertedIndex.getDocIds(dictId)
                .forEach((IntConsumer) flattenedDocId -> docIds.add(getDocId(flattenedDocId,
                    flattenedDocIdUpperBounds)));
            invertedIndexWriter.add(docIds);
          }
        }
      }
      return true;
    }
  }

  private int countDirectDocIdPostingLists(StringDictionary dictionary) {
    int count = 0;
    byte[] dictBuffer = dictionary.getBuffer();
    for (int dictId = 0, length = dictionary.length(); dictId < length; dictId++) {
      if (JsonIndexUtils.isDirectDocIdIndexEligibleValue(dictionary.getStringValue(dictId, dictBuffer))) {
        count++;
      }
    }
    return count;
  }

  private int[] buildFlattenedDocIdUpperBounds() {
    int numDocs = _numFlattenedRecordsList.size();
    int[] flattenedDocIdUpperBounds = new int[numDocs];
    int flattenedDocIdUpperBound = 0;
    for (int docId = 0; docId < numDocs; docId++) {
      flattenedDocIdUpperBound += _numFlattenedRecordsList.getInt(docId);
      flattenedDocIdUpperBounds[docId] = flattenedDocIdUpperBound;
    }
    return flattenedDocIdUpperBounds;
  }

  private static int getDocId(int flattenedDocId, int[] flattenedDocIdUpperBounds) {
    int target = flattenedDocId + 1;
    int low = 0;
    int high = flattenedDocIdUpperBounds.length;
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (flattenedDocIdUpperBounds[mid] < target) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }
}
