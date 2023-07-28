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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.text.AbstractTextIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.FSTHeader;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

import static java.nio.charset.StandardCharsets.UTF_8;


public class NativeTextIndexCreator extends AbstractTextIndexCreator {
  private static final String TEMP_DIR_SUFFIX = ".nativetext.idx.tmp";
  private static final String FST_FILE_NAME = "native.fst";
  private static final String INVERTED_INDEX_FILE_NAME = "inverted.index.buf";

  /*
   * MAGIC HEADER (4 bytes)
   * VERSION (4 bytes)
   * FST size (4 bytes)
   * Inverted index size (8 bytes)
   * Number of bitmaps (4 bytes)
   */
  public static final int HEADER_LENGTH = 24;
  public static final int VERSION = 1;

  private final String _columnName;
  private final FSTBuilder _fstBuilder;
  private final File _indexFile;
  private final File _tempDir;
  private final File _fstIndexFile;
  private final File _invertedIndexFile;
  private final Map<String, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  private final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();
  private int _nextDocId = 0;
  private int _fstDataSize;
  private int _numBitMaps;

  public NativeTextIndexCreator(String column, File indexDir)
      throws IOException {
    _columnName = column;
    _fstBuilder = new FSTBuilder();
    _indexFile = new File(indexDir, column + V1Constants.Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, column + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _fstIndexFile = new File(_tempDir, FST_FILE_NAME);
    _invertedIndexFile = new File(_tempDir, INVERTED_INDEX_FILE_NAME);
  }

  @Override
  public void add(String document) {
    addHelper(document);
    _nextDocId++;
  }

  @Override
  public void add(String[] documents, int length) {
    for (int i = 0; i < length; i++) {
      addHelper(documents[i]);
    }
    _nextDocId++;
  }

  private void addHelper(String document) {
    List<String> tokens;
    try {
      tokens = analyze(document, new StandardAnalyzer(LuceneTextIndexCreator.ENGLISH_STOP_WORDS_SET));
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    for (String token : tokens) {
      addToPostingList(token);
    }
  }

  @Override
  public void seal()
      throws IOException {
    int dictId = 0;
    int numPostingLists = _postingListMap.size();
    try (BitmapInvertedIndexWriter invertedIndexWriter = new BitmapInvertedIndexWriter(_invertedIndexFile,
        numPostingLists)) {

      for (Map.Entry<String, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
        byte[] byteArray = entry.getKey().getBytes(UTF_8);
        _fstBuilder.add(byteArray, 0, byteArray.length, dictId++);
        invertedIndexWriter.add(entry.getValue().get());
      }
    }

    FST fst = _fstBuilder.complete();
    _fstDataSize = fst.save(new FileOutputStream(_fstIndexFile));
    generateIndexFile();
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  public List<String> analyze(String text, Analyzer analyzer)
      throws IOException {
    List<String> result = new ArrayList<>();
    TokenStream tokenStream = analyzer.tokenStream(_columnName, text);
    CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      result.add(attr.toString());
    }
    return result;
  }

  /**
   * Adds the given value to the posting list.
   */
  void addToPostingList(String value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(value, bitmapWriter);
      _numBitMaps++;
    }
    bitmapWriter.add(_nextDocId);
  }

  private void generateIndexFile()
      throws IOException {
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    headerBuffer.putInt(FSTHeader.FST_MAGIC);
    headerBuffer.putInt(VERSION);
    headerBuffer.putInt(_fstDataSize);
    long invertedIndexFileLength = _invertedIndexFile.length();
    headerBuffer.putLong(invertedIndexFileLength);
    headerBuffer.putInt(_numBitMaps);
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel invertedIndexFileChannel = new RandomAccessFile(_invertedIndexFile, "r").getChannel();
        FileChannel fstFileChannel = new RandomAccessFile(_fstIndexFile, "rw").getChannel()) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(fstFileChannel, 0, _fstDataSize, indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(invertedIndexFileChannel, 0, invertedIndexFileLength,
          indexFileChannel);
    }
  }
}
