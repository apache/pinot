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
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;

public final class UnSortedSingleValueIterator extends BlockSingleValIterator {

  private int counter = 0;
  private ColumnMetadata columnMetadata;
  private SingleColumnSingleValueReader sVReader;


  public UnSortedSingleValueIterator(SingleColumnSingleValueReader sVReader,
      ColumnMetadata columnMetadata) {
    super();
    this.sVReader = sVReader;
    this.columnMetadata = columnMetadata;
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
  public int nextIntVal() {
    if (counter >= columnMetadata.getTotalDocs()) {
      return Constants.EOF;
    }

    return sVReader.getInt(counter++);
  }

  @Override
  public String nextStringVal() {
    if (counter >= columnMetadata.getTotalDocs()) {
      return null;
    }
    return sVReader.getString(counter++);
  }

  @Override
  public boolean reset() {
    counter = 0;
    return true;
  }

  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean hasNext() {
    return (counter < columnMetadata.getTotalDocs());
  }

  @Override
  public DataType getValueType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int currentDocId() {
    return counter;
  }
}
