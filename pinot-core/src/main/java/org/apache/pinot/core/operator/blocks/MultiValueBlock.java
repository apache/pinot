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
package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.operator.docvalsets.MultiValueSet;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


@SuppressWarnings("rawtypes")
public final class MultiValueBlock implements Block {
  private final BlockValSet _blockValSet;
  private final BlockMetadata _blockMetadata;

  public MultiValueBlock(SingleColumnMultiValueReader reader, int numDocs, int maxNumMultiValues,
      FieldSpec.DataType dataType, Dictionary dictionary) {
    _blockValSet = new MultiValueSet(reader, dataType);
    _blockMetadata = new BlockMetadataImpl(numDocs, false, maxNumMultiValues, dataType, dictionary);
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return _blockValSet;
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    return _blockMetadata;
  }
}
