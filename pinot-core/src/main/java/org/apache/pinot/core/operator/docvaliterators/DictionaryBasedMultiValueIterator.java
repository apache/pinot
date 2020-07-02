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

import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;


@SuppressWarnings("unchecked")
public final class DictionaryBasedMultiValueIterator extends BlockMultiValIterator {

  private final SingleColumnMultiValueReader _reader;
  private final int _numDocs;
  private final ReaderContext _context;
  private final Dictionary _dictionary;
  private final int[] _dictIds;

  private int _nextDocId;

  public DictionaryBasedMultiValueIterator(SingleColumnMultiValueReader reader, Dictionary dictionary, int numDocs,
      int maxLength) {
    _reader = reader;
    _numDocs = numDocs;
    _context = _reader.createContext();
    _dictionary = dictionary;
    _dictIds = new int[maxLength];
  }

  @Override
  public int nextIntVal(int[] intArray) {
    int length = _reader.getIntArray(_nextDocId++, _dictIds, _context);
    for (int i = 0; i < length; i++) {
      intArray[i] = _dictionary.getIntValue(_dictIds[i]);
    }
    return length;
  }

  @Override
  public int nextDoubleVal(double[] doubleArray) {
    int length = _reader.getIntArray(_nextDocId++, _dictIds, _context);
    for (int i = 0; i < length; i++) {
      doubleArray[i] = _dictionary.getDoubleValue(_dictIds[i]);
    }
    return length;
  }

  @Override
  public int nextFloatVal(float[] floatArray) {
    int length = _reader.getIntArray(_nextDocId++, _dictIds, _context);
    for (int i = 0; i < length; i++) {
      floatArray[i] = _dictionary.getFloatValue(_dictIds[i]);
    }
    return length;
  }

  @Override
  public int nextLongVal(long[] longArray) {
    int length = _reader.getIntArray(_nextDocId++, _dictIds, _context);
    for (int i = 0; i < length; i++) {
      longArray[i] = _dictionary.getLongValue(_dictIds[i]);
    }
    return length;
  }

  @Override
  public int nextBytesArrayVal(byte[][] bytesArrays) {
    int length = _reader.getIntArray(_nextDocId++, _dictIds, _context);
    for (int i = 0; i < length; i++) {
      bytesArrays[i] = _dictionary.getBytesValue(_dictIds[i]);
    }
    return length;
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
