/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.reader.impl;

import java.io.IOException;

import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.ByteBufferBinarySearchUtil;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;

public class SortedForwardIndexReader
    extends BaseSingleColumnSingleValueReader<SortedValueReaderContext> {
  private final FixedByteSingleValueMultiColReader indexReader;
  private final ByteBufferBinarySearchUtil fileBinarySearcher;
  private final int numDocs;

  public SortedForwardIndexReader(FixedByteSingleValueMultiColReader rawFileReader, int numDocs) {
    indexReader = rawFileReader;
    fileBinarySearcher = new ByteBufferBinarySearchUtil(indexReader);
    this.numDocs = numDocs;
  }

  @Override
  public void close() throws IOException {
    // no need to close here , will be closed by parent container

  }

  @Override
  public char getChar(int row) {
    throw new UnsupportedOperationException("not allowed in sorted reader");
  }

  @Override
  public short getShort(int row) {
    throw new UnsupportedOperationException("not allowed in sorted reader");
  }

  @Override
  public int getInt(int docId, SortedValueReaderContext context) {
    if (docId >= context.docIdStart && docId <= context.docIdEnd) {
      return context.value;
    } else {
      int value = getInt(docId);
      if (value != Constants.EOF) {
        context.value = value;
        context.docIdStart = indexReader.getInt(context.value, 0);
        context.docIdEnd = indexReader.getInt(context.value, 1);
      }
      return value;
    }

  }

  @Override
  public int getInt(int docId) {
    if (indexReader.getNumberOfRows() == 1) {
      return 0;
    }
    if (indexReader.getInt(0, 1) >= docId) {
      return 0;
    }
    int ret = fileBinarySearcher.binarySearch(1, docId, 1, indexReader.getNumberOfRows());

    if (ret < 0) {
      ret = (ret + 1) * -1;
    }

    if (ret < indexReader.getNumberOfRows()) {
      return ret;
    }

    return Constants.EOF;
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    int endPos = rowStartPos + rowSize;
    for (int ri = rowStartPos; ri < endPos; ++ri) {
      values[valuesStartPos++] = getInt(rows[ri]);
    }
  }

  public int getLength() {
    return numDocs;
  }

  @Override
  public SortedValueReaderContext createContext() {
    return new SortedValueReaderContext();
  }
}
