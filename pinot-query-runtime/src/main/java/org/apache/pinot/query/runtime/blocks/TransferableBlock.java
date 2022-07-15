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

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.core.common.datablock.ColumnarDataBlock;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.common.datablock.DataBlockUtils;
import org.apache.pinot.core.common.datablock.RowDataBlock;


/**
 * A {@code TransferableBlock} is a wrapper around {@link BaseDataBlock} for transferring data using
 * {@link org.apache.pinot.common.proto.Mailbox}.
 */
public class TransferableBlock implements Block {

  private final BaseDataBlock.Type _type;
  private final DataSchema _dataSchema;
  private final boolean _isErrorBlock;

  private BaseDataBlock _dataBlock;
  private List<Object[]> _container;

  public TransferableBlock(List<Object[]> container, DataSchema dataSchema, BaseDataBlock.Type containerType) {
    this(container, dataSchema, containerType, false, false);
  }

  public TransferableBlock(List<Object[]> container, DataSchema dataSchema, BaseDataBlock.Type containerType,
      boolean isEndOfStreamBlock, boolean isErrorBlock) {
    _container = container;
    _dataSchema = dataSchema;
    _type = containerType;
    _isErrorBlock = isErrorBlock;
  }

  public TransferableBlock(BaseDataBlock dataBlock) {
    _dataBlock = dataBlock;
    _dataSchema = dataBlock.getDataSchema();
    _type = dataBlock instanceof ColumnarDataBlock ? BaseDataBlock.Type.COLUMNAR
        : dataBlock instanceof RowDataBlock ? BaseDataBlock.Type.ROW : BaseDataBlock.Type.METADATA;
    _isErrorBlock = !_dataBlock.getExceptions().isEmpty();
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  public List<Object[]> getContainer() {
    if (_container == null) {
      switch (_type) {
        case ROW:
          _container = DataBlockUtils.extraRows(_dataBlock);
          break;
        case COLUMNAR:
        default:
          throw new UnsupportedOperationException("Unable to extract from container with type: " + _type);
      }
    }
    return _container;
  }

  public BaseDataBlock getDataBlock() {
    if (_dataBlock == null) {
      try {
        switch (_type) {
          case ROW:
            _dataBlock = DataBlockBuilder.buildFromRows(_container, null, _dataSchema);
            break;
          case COLUMNAR:
            _dataBlock = DataBlockBuilder.buildFromColumns(_container, null, _dataSchema);
            break;
          case METADATA:
            throw new UnsupportedOperationException("Metadata block cannot be constructed from container");
          default:
            throw new UnsupportedOperationException("Unable to build from container with type: " + _type);
        }
      } catch (Exception e) {
        throw new RuntimeException("Unable to create DataBlock");
      }
    }
    return _dataBlock;
  }

  public BaseDataBlock.Type getType() {
    return _type;
  }

  public boolean isEndOfStreamBlock() {
    return _type == BaseDataBlock.Type.METADATA;
  }

  public boolean isErrorBlock() {
    return _isErrorBlock;
  }

  public byte[] toBytes()
      throws IOException {
    return _dataBlock.toBytes();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }
}
