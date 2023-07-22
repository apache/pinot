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
package org.apache.pinot.segment.local.segment.index.readers.constant;

import java.util.List;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.spi.utils.Pairs;


/**
 * Sorted index reader for single-value column with constant values.
 */
public final class ConstantSortedIndexReader implements SortedIndexReader<ForwardIndexReaderContext> {
  private final int _numDocs;

  public ConstantSortedIndexReader(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public List<ForwardIndexByteRange> getForwardIndexByteRange(int docId, ForwardIndexReaderContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDictId(int docId, ForwardIndexReaderContext context) {
    return 0;
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    for (int i = 0; i < length; i++) {
      dictIdBuffer[i] = 0;
    }
  }

  @Override
  public Pairs.IntPair getDocIds(int dictId) {
    return new Pairs.IntPair(0, _numDocs - 1);
  }

  @Override
  public void close() {
  }
}
