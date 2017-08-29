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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;

public final class MultiValueIterator extends BlockMultiValIterator {

  private int counter = 0;
  private ColumnMetadata columnMetadata;
  private SingleColumnMultiValueReader mVReader;
  private ReaderContext context;

  public MultiValueIterator(SingleColumnMultiValueReader mVReader, ColumnMetadata columnMetadata) {
    super();
    this.mVReader = mVReader;
    this.columnMetadata = columnMetadata;
    this.context = mVReader.createContext();
  }

  @Override
  public int nextIntVal(int[] intArray) {
    return mVReader.getIntArray(counter++, intArray, context);
  }

  @Override
  public boolean skipTo(int docId) {
    if (docId >= columnMetadata.getTotalDocs()) {
      return false;
    }
    counter = docId;
    return true;
  }

  @Override
  public int size() {
    return columnMetadata.getTotalDocs();
  }

  @Override
  public boolean reset() {
    counter = 0;
    return true;
  }

  @Override
  public boolean next() {
    return false;
  }

  @Override
  public boolean hasNext() {
    return (counter < columnMetadata.getTotalDocs());
  }

  @Override
  public DataType getValueType() {
    return columnMetadata.getDataType();
  }

  @Override
  public int currentDocId() {
    return counter;
  }
}
