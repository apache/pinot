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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFSTImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RealtimeNativeTextIndex implements MutableTextIndex {
  private final String _column;
  private final MutableFST _mutableFST;
  private final RealtimeInvertedIndex _invertedIndex;
  private final Map<String, Integer> _termToDictIdMapping;
  private int _nextDocId = 0;
  private int _nextDictId = 0;
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  public RealtimeNativeTextIndex(String column) {
    _column = column;
    _mutableFST = new MutableFSTImpl();
    _termToDictIdMapping = new HashMap<>();
    _invertedIndex = new RealtimeInvertedIndex();

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  @Override
  public void add(String document) {
    List<String> tokens;
    try {
      tokens = analyze(document, new StandardAnalyzer(LuceneTextIndexCreator.ENGLISH_STOP_WORDS_SET));
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    try {
      _writeLock.lock();
      for (String token : tokens) {
        Integer currentDictId = _termToDictIdMapping.get(token);
        if (currentDictId == null) {

          currentDictId = _nextDictId;
          _mutableFST.addPath(token, currentDictId);
          _termToDictIdMapping.put(token, currentDictId);
          ++_nextDictId;
        }
        addToPostingList(currentDictId);
      }
      _nextDocId++;
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    MutableRoaringBitmap matchingDocIds = null;
    try {
      _readLock.lock();
      RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
      RealTimeRegexpMatcher.regexMatch(searchQuery, _mutableFST, writer::add);
      ImmutableRoaringBitmap matchingDictIds = writer.get();

      for (PeekableIntIterator it = matchingDictIds.getIntIterator(); it.hasNext(); ) {
        int dictId = it.next();
        if (dictId >= 0) {
          ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
          if (matchingDocIds == null) {
            matchingDocIds = docIds.toMutableRoaringBitmap();
          } else {
            matchingDocIds.or(docIds);
          }
        }
      }
    } finally {
      _readLock.unlock();
    }

    return matchingDocIds == null ? new MutableRoaringBitmap() : matchingDocIds;
  }

  @Override
  public void close()
      throws IOException {
  }

  public List<String> analyze(String text, Analyzer analyzer)
      throws IOException {
    List<String> result = new ArrayList<>();
    TokenStream tokenStream = analyzer.tokenStream(_column, text);
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
  void addToPostingList(int dictId) {
    _invertedIndex.add(dictId, _nextDocId);
  }
}
