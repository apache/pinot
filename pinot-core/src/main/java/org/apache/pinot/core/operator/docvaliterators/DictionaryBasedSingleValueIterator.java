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
package org.apache.pinot.core.operator.docvaliterators;

import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;


@SuppressWarnings("unchecked")
public final class DictionaryBasedSingleValueIterator extends BlockSingleValIterator {

  private final SingleColumnSingleValueReader _reader;
  private final int _numDocs;
  private final ReaderContext _context;
  private final Dictionary _dictionary;

  private int _nextDocId;

  public DictionaryBasedSingleValueIterator(SingleColumnSingleValueReader reader, Dictionary dictionary, int numDocs) {
    _reader = reader;
    _numDocs = numDocs;
    _context = _reader.createContext();
    _dictionary = dictionary;
  }

  @Override
  public int nextIntVal() {
    return _dictionary.getIntValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public long nextLongVal() {
    return _dictionary.getLongValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public float nextFloatVal() {
    return _dictionary.getFloatValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public double nextDoubleVal() {
    return _dictionary.getDoubleValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public String nextStringVal() {
    return _dictionary.getStringValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public byte[] nextBytesVal() {
    return _dictionary.getBytesValue(_reader.getInt(_nextDocId++, _context));
  }

  @Override
  public boolean hasNext() {
    return _nextDocId < _numDocs;
  }

  @Override
  public void skipTo(int docId) {
    _nextDocId = docId;
  }

  @Override
  public void reset() {
    _nextDocId = 0;
  }
}
