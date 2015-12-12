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
package com.linkedin.pinot.core.operator.docvaliterators;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.segment.index.readers.SortedForwardIndexReader;

public final class SortedSingleValueIterator extends BlockSingleValIterator {

  private int counter = 0;
  private SortedForwardIndexReader sVReader;

  public SortedSingleValueIterator(SortedForwardIndexReader sVReader) {
    this.sVReader = sVReader;
  }

  @Override
  public boolean skipTo(int docId) {
    if (docId >= sVReader.getLength()) {
      return false;
    }

    counter = docId;

    return true;
  }

  @Override
  public int size() {
    return sVReader.getLength();
  }

  @Override
  public int nextIntVal() {
    if (counter >= sVReader.getLength()) {
      return Constants.EOF;
    }
    return sVReader.getInt(counter++);
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
    return (counter < sVReader.getLength());
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