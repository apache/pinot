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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.operator.docvalsets.SingleValueSet;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public final class SingleValueBlock implements Block {
  private final SingleColumnSingleValueReader<? super ReaderContext> _reader;
  private final BlockValSet _blockValSet;
  private final BlockMetadata _blockMetadata;

  public SingleValueBlock(SingleColumnSingleValueReader<? super ReaderContext> reader, int numDocs,
      FieldSpec.DataType dataType, Dictionary dictionary) {
    _reader = reader;
    _blockValSet = new SingleValueSet(reader, numDocs, dataType);
    _blockMetadata = new BlockMetadataImpl(numDocs, true, 0, dataType, dictionary);
  }

  public SingleColumnSingleValueReader<? extends ReaderContext> getReader() {
    return _reader;
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
