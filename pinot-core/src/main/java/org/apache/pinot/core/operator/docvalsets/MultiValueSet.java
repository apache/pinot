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
package org.apache.pinot.core.operator.docvalsets;

import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.BaseBlockValSet;
import org.apache.pinot.core.common.BlockValIterator;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.operator.docvaliterators.MultiValueIterator;


public final class MultiValueSet extends BaseBlockValSet {
  private final SingleColumnMultiValueReader _reader;
  private final int _numDocs;
  private final DataType _dataType;

  public MultiValueSet(SingleColumnMultiValueReader reader, int numDocs, DataType dataType) {
    _reader = reader;
    _numDocs = numDocs;
    _dataType = dataType;
  }

  @Override
  public BlockValIterator iterator() {
    return new MultiValueIterator(_reader, _numDocs);
  }

  @Override
  public DataType getValueType() {
    return _dataType;
  }
}
