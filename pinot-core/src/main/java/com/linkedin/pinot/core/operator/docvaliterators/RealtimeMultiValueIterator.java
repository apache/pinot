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
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;

public final class RealtimeMultiValueIterator extends BlockMultiValIterator {
 

  SingleColumnMultiValueReader reader;
  
  private int counter = 0;
  private int max;// = docIdSearchableOffset + 1;
  private DataType dataType;
  public RealtimeMultiValueIterator(SingleColumnMultiValueReader reader, int max, DataType dataType){
    this.reader = reader;
    this.max = max;
    this.dataType = dataType;
  }
  @Override
  public boolean skipTo(int docId) {
    if (docId > max) {
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
    return false;
  }

  @Override
  public int nextIntVal(int[] intArray) {
    if (!hasNext()) {
      return Constants.EOF;
    }

    
    return reader.getIntArray(counter++, intArray);
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