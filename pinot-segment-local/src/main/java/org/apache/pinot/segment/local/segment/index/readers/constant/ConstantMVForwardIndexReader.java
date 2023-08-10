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
package org.apache.pinot.segment.local.segment.index.readers.constant;

import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dictionary-encoded forward index reader for multi-value column with constant values.
 */
public final class ConstantMVForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getStoredType() {
    return DataType.INT;
  }

  /*
   * Asserting on dictIdBuffer being non-empty is done on purpose here as this reader is used in the query hot path and
   * it would be prohibitively expensive.
   * It is always assumed that the dictIdBuffer is set correctly based on the maxNumberOfMultiValues column metadata.
   */
  @Override
  public int getDictIdMV(int docId, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    dictIdBuffer[0] = 0;
    return 1;
  }

  @Override
  public int[] getDictIdMV(int docId, ForwardIndexReaderContext context) {
    return new int[]{0};
  }

  @Override
  public int getNumValuesMV(int docId, ForwardIndexReaderContext context) {
    return 1;
  }

  @Override
  public void close() {
  }
}
