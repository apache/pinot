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
package com.linkedin.pinot.util.datasource;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;

public final class ArrayBasedDocValIterator extends BlockSingleValIterator {
  int counter = 0;
  int[] values;

  public ArrayBasedDocValIterator(int[] values) {
    this.values = values;
  }

  @Override
  public boolean skipTo(int docId) {
    if (docId >= values.length) {
      return false;
    }
    counter = docId;
    return true;
  }

  @Override
  public int size() {
    return values.length;
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
  public int nextIntVal() {
    return values[counter++];
  }

  @Override
  public boolean hasNext() {
    return counter < values.length;
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public int currentDocId() {
    // TODO Auto-generated method stub
    return 0;
  }
}
