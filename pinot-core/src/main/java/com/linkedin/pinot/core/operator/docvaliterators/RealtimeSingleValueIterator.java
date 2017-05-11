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

public final class RealtimeSingleValueIterator extends BlockSingleValIterator {

  private int counter = 0;
  private SingleColumnSingleValueReader reader;
  private int max; 
  private DataType dataType;

  public RealtimeSingleValueIterator(SingleColumnSingleValueReader reader, int max,
      DataType dataType) {
    super();
    this.reader = reader;
    this.max = max;
    this.dataType = dataType;
  }

  @Override
  public boolean skipTo(int docId) {
    if (docId >= max) {
      return false;
    }
    counter = docId;
    return true;
  }

  @Override
  public int size() {
    return max;
  }

  @Override
  public boolean reset() {
    counter = 0;
    return true;
  }

  @Override
  public boolean next() {
    return counter < max;
  }

  @Override
  public int nextIntVal() {
    if (!hasNext()) {
      return Constants.EOF;
    }
    int ret = reader.getInt(counter);
    counter++;
    return ret;
  }

  @Override
  public long nextLongVal() {
    if (!hasNext()) {
      return Constants.EOF;
    }

    long ret = reader.getLong(counter);
    return ret;
  }

  @Override
  public float nextFloatVal() {
    if (!hasNext()) {
      return Constants.EOF;
    }

    float ret = reader.getFloat(counter);
    return ret;
  }

  @Override
  public double nextDoubleVal() {
    if (!hasNext()) {
      return Constants.EOF;
    }

    double ret = reader.getDouble(counter);
    return ret;
  }

  @Override
  public boolean hasNext() {
    return (counter < max);
  }

  @Override
  public DataType getValueType() {
    return dataType;
  }

  @Override
  public int currentDocId() {
    return counter;
  }
}
