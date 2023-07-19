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
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Base implementation of the json index creator.
 * <p>Header format:
 * <ul>
 *   <li>Version (int)</li>
 *   <li>Max value length (int)</li>
 *   <li>Dictionary file length (long)</li>
 *   <li>Inverted index file length (long)</li>
 *   <li>Doc id mapping file length (long)</li>
 * </ul>
 */
public abstract class BaseJsonIndexCreator implements JsonIndexCreator {
  // NOTE: V1 is deprecated because it does not support top-level value, top-level array and nested array
  public static final int VERSION_1 = 1;
  public static final int VERSION_2 = 2;
  public static final int HEADER_LENGTH = 32;

  static final String TEMP_DIR_SUFFIX = ".json.idx.tmp";
  static final String DICTIONARY_FILE_NAME = "dictionary.buf";
  static final String INVERTED_INDEX_FILE_NAME = "inverted.index.buf";

  final JsonIndexConfig _jsonIndexConfig;
  final File _indexFile;
  final File _tempDir;
  final File _dictionaryFile;
  final File _invertedIndexFile;
  final IntList _numFlattenedRecordsList = new IntArrayList();
  final Map<String, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();

  int _nextFlattenedDocId;
  int _maxValueLength;

  BaseJsonIndexCreator(File indexDir, String columnName, JsonIndexConfig jsonIndexConfig)
      throws IOException {
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
  }

  @Override
  public void add(String jsonString)
      throws IOException {
    addFlattenedRecords(JsonUtils.flatten(JsonUtils.stringToJsonNode(jsonString), _jsonIndexConfig));
  }

  /**
   * Adds the flattened records for the next document.
   */
  void addFlattenedRecords(List<Map<String, String>> records)
      throws IOException {
    int numRecords = records.size();
    Preconditions.checkState(_nextFlattenedDocId + numRecords >= 0, "Got more than %s flattened records",
        Integer.MAX_VALUE);
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
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    headerBuffer.putInt(VERSION_2);
    headerBuffer.putInt(_maxValueLength);
    long dictionaryFileLength = _dictionaryFile.length();
    long invertedIndexFileLength = _invertedIndexFile.length();
    long docIdMappingFileLength = (long) _nextFlattenedDocId << 2;
    headerBuffer.putLong(dictionaryFileLength);
    headerBuffer.putLong(invertedIndexFileLength);
    headerBuffer.putLong(docIdMappingFileLength);
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel dictionaryFileChannel = new RandomAccessFile(_dictionaryFile, "r").getChannel();
        FileChannel invertedIndexFileChannel = new RandomAccessFile(_invertedIndexFile, "r").getChannel()) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(dictionaryFileChannel, 0, dictionaryFileLength,
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(invertedIndexFileChannel, 0, invertedIndexFileLength,
          indexFileChannel);

      // Write the doc id mapping to the index file
      ByteBuffer docIdMappingBuffer =
          indexFileChannel.map(FileChannel.MapMode.READ_WRITE, indexFileChannel.position(), docIdMappingFileLength)
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
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }
}
