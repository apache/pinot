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
package org.apache.pinot.query.runtime.blocks;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.util.DataBlockExtractUtils;

/// A block that contains data in serialized format.
///
/// This class is a subclass of [MseBlock.Data] and is used to store data in serialized format.
/// This is the most efficient way to store data, but it is also the hardest to work with.
/// As the day this comment was written, this class is only used when we need to shuffle data through the network.
/// In all other cases, we use [RowHeapDataBlock].
public class SerializedDataBlock implements MseBlock.Data {
  private final DataBlock _dataBlock;

  /// Creates a new block with the given data block.
  /// @param dataBlock The data block to store in this block. It cannot be a metadata block.
  /// @throws IllegalArgumentException If the data block is a metadata block.
  public SerializedDataBlock(DataBlock dataBlock) {
    Preconditions.checkArgument(dataBlock.getDataBlockType() != DataBlock.Type.METADATA,
        "SerializedDataBlock cannot be used to decorate metadata block");
    _dataBlock = dataBlock;
  }

  /// Returns the data block stored in this block.
  /// It is guaranteed that the returned data block is not a metadata block.
  public DataBlock getDataBlock() {
    return _dataBlock;
  }

  @Override
  public int getNumRows() {
    return _dataBlock.getNumberOfRows();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataBlock.getDataSchema();
  }

  @Override
  public RowHeapDataBlock asRowHeap() {
    List<Object[]> rows = DataBlockExtractUtils.extractRows(_dataBlock);
    return new RowHeapDataBlock(rows, _dataBlock.getDataSchema(), null);
  }

  @Override
  public SerializedDataBlock asSerialized() {
    return this;
  }

  @Override
  public boolean isRowHeap() {
    return false;
  }

  @Override
  public <R, A> R accept(Visitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }
}
