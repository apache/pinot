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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * RealtimeNgramFilteringIndex is a mutable inverted index that supports adding document IDs for n-grams. An n-gram
 * is a contiguous sequence of n characters from input text. The length of the n-gram is variable between
 * [minNgramLength, maxNgramLength] as specified in the index config. Internally, it uses a realtime inverted index
 * which maps n-grams to their posting lists (document IDs). The index is thread-safe for single writer and multiple
 * readers.
 */
public class RealtimeNgramFilteringIndex implements MutableTextIndex {
  private final String _column;
  // Under the hood, ngram index is implemented as an inverted index from n-grams to their posting lists.
  private final RealtimeInvertedIndex _invertedIndex;
  // A mapping from n-grams to their dictionary IDs.
  private final Object2IntOpenHashMap<String> _ngramToDictIdMapping;
  // Read and write locks for thread safety.
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  // Next document ID to be assigned.
  private int _nextDocId = 0;
  private int _nextDictId = 0;

  private final int _minNgramLength;
  private final int _maxNgramLength;

  public RealtimeNgramFilteringIndex(String column, int minNgramLength, int maxNgramLength) {
    _column = column;
    _invertedIndex = new RealtimeInvertedIndex();
    _ngramToDictIdMapping = new Object2IntOpenHashMap<>();
    _minNgramLength = minNgramLength;
    _maxNgramLength = maxNgramLength;

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  /**
   * Index the string value
   *
   * @param value the input value as a string
   */
  @Override
  public void add(String value) {
    if (value == null) {
      return;
    }
    addHelper(value);
    _nextDocId++;
  }

  /**
   * Index an array of string values. Each string in the array is treated as a separate document.
   *
   * @param values the documents as an array of strings
   */
  @Override
  public void add(String[] values) {
    if (values == null || values.length == 0) {
      return;
    }
    for (String value : values) {
      if (value != null) {
        addHelper(value);
      }
    }
    _nextDocId++;
  }

  /**
   * Returns the matching dictionary ids for the given search query (optional).
   *
   * @param searchQuery as a literal string
   */
  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the matching document ids for the given search query.
   * Returns null if no n-grams are generated from the search query -- either because the search query is null or empty
   * or shorter than the minimum n-gram length.
   * @param searchQuery as a literal string
   */
  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    _readLock.lock();
    Iterable<String> ngrams = generateNgrams(searchQuery);
    if (!ngrams.iterator().hasNext()) {
      return null; // No n-grams generated, return null.
    }
    MutableRoaringBitmap resultBitmap = null;
    try {
      for (String ngram : ngrams) {
        int dictId = _ngramToDictIdMapping.getInt(ngram);
        if (resultBitmap == null) {
          resultBitmap = _invertedIndex.getDocIds(dictId);
        } else {
          resultBitmap.and(_invertedIndex.getDocIds(dictId));
        }
      }
    } finally {
      _readLock.unlock();
    }
    return resultBitmap;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If the stream is already closed then
   * invoking this method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised to relinquish the underlying resources and to
   * internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close()
      throws IOException {
  }

  private void addHelper(String value) {
    Iterable<String> ngrams = generateNgrams(value);
    _writeLock.lock();
    try {
      for (String ngram : ngrams) {
        int currentDictId = _ngramToDictIdMapping.computeIfAbsent(ngram, k -> _nextDictId++);
        _invertedIndex.add(currentDictId, _nextDocId);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  // Use Lucene's NGramTokenizer to generate n-grams from the input value.
  private Iterable<String> generateNgrams(String value) {
    if (value == null || value.isEmpty()) {
      return java.util.Collections.emptyList();
    }
    ArrayList<String> ngrams = new ArrayList<>();
    // Implement the logic to generate n-grams from the input value.
    try (NGramTokenizer nGramTokenizer = new NGramTokenizer(_minNgramLength, _maxNgramLength)) {
      nGramTokenizer.setReader(new java.io.StringReader(value));
      nGramTokenizer.reset();
      while (nGramTokenizer.incrementToken()) {
        // Extract the text term as a string and add it to the list.
        ngrams.add(nGramTokenizer.getAttribute(CharTermAttribute.class).toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ngrams;
  }
}
