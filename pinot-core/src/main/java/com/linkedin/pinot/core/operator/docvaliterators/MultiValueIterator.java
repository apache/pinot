/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;


@SuppressWarnings("unchecked")
public final class MultiValueIterator extends BlockMultiValIterator {
  private final SingleColumnMultiValueReader _reader;
  private final int _numDocs;
  private final ReaderContext _context;

  private int _nextDocId;

  public MultiValueIterator(SingleColumnMultiValueReader reader, int numDocs) {
    _reader = reader;
    _numDocs = numDocs;
    _context = _reader.createContext();
  }

  @Override
  public int nextIntVal(int[] intArray) {
    return _reader.getIntArray(_nextDocId++, intArray, _context);
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
