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
package com.linkedin.pinot.core.operator.docvaliterators;

import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;


public final class SingleValueIterator extends BlockSingleValIterator {
  private SingleColumnSingleValueReader<? super ReaderContext> _reader;
  private final int _numDocs;
  private final ReaderContext _context;
  private int _nextDocId;

  public SingleValueIterator(SingleColumnSingleValueReader<? super ReaderContext> reader, int numDocs) {
    _reader = reader;
    _numDocs = numDocs;
    _context = _reader.createContext();
    _nextDocId = 0;
  }

  @Override
  public int nextIntVal() {
    return _reader.getInt(_nextDocId++, _context);
  }

  @Override
  public long nextLongVal() {
    return _reader.getLong(_nextDocId++, _context);
  }

  @Override
  public float nextFloatVal() {
    return _reader.getFloat(_nextDocId++, _context);
  }

  @Override
  public double nextDoubleVal() {
    return _reader.getDouble(_nextDocId++, _context);
  }

  @Override
  public String nextStringVal() {
    return _reader.getString(_nextDocId++, _context);
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
