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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.runtime.operator.OperatorStats;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;

/**
 * A {@code TransferableBlock} is a wrapper around {@link DataBlock} for transferring data using
 * {@link org.apache.pinot.common.proto.Mailbox}.
 */
public class TransferableBlock implements Block {
  private final DataBlock.Type _type;
  private final DataSchema _dataSchema;
  private final int _numRows;

  private DataBlock _dataBlock;
  private List<Object[]> _container;

  public TransferableBlock(List<Object[]> container, DataSchema dataSchema, DataBlock.Type containerType) {
    _container = container;
    _dataSchema = dataSchema;
    _type = containerType;
    _numRows = _container.size();
  }

  public TransferableBlock(DataBlock dataBlock) {
    _dataBlock = dataBlock;
    _dataSchema = dataBlock.getDataSchema();
    _type = dataBlock instanceof ColumnarDataBlock ? DataBlock.Type.COLUMNAR
        : dataBlock instanceof RowDataBlock ? DataBlock.Type.ROW : DataBlock.Type.METADATA;
    _numRows = _dataBlock.getNumberOfRows();
  }

  public Map<String, OperatorStats> getResultMetadata() {
    if (isSuccessfulEndOfStreamBlock()) {
      return OperatorUtils.getOperatorStatsFromMetadata((MetadataBlock) _dataBlock);
    }
    return new HashMap<>();
  }

  public int getNumRows() {
    return _numRows;
  }

  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Retrieve the extracted {@link TransferableBlock#_container} of the transferable block.
   * If not already constructed. It will use {@link DataBlockUtils} to extract the row/columnar data from the
   * binary-packed format.
   *
   * @return data container.
   */
  public List<Object[]> getContainer() {
    if (_container == null) {
      switch (_type) {
        case ROW:
          _container = DataBlockUtils.extractRows(_dataBlock, ObjectSerDeUtils::deserialize);
          break;
        case COLUMNAR:
        case METADATA:
        default:
          throw new UnsupportedOperationException("Unable to extract from container with type: " + _type);
      }
    }
    return _container;
  }

  /**
   * Retrieve the binary-packed version of the data block.
   * If not already constructed. It will use {@link DataBlockBuilder} to construct the binary-packed format from
   * the {@link TransferableBlock#_container}.
   *
   * @return data block.
   */
  public DataBlock getDataBlock() {
    if (_dataBlock == null) {
      try {
        switch (_type) {
          case ROW:
            _dataBlock = DataBlockBuilder.buildFromRows(_container, _dataSchema);
            break;
          case COLUMNAR:
            _dataBlock = DataBlockBuilder.buildFromColumns(_container, _dataSchema);
            break;
          case METADATA:
            throw new UnsupportedOperationException("Metadata block cannot be constructed from container");
          default:
            throw new UnsupportedOperationException("Unable to build from container with type: " + _type);
        }
      } catch (Exception e) {
        throw new RuntimeException("Unable to create DataBlock", e);
      }
    }
    return _dataBlock;
  }

  /**
   * Return the type of block (one of ROW, COLUMNAR, or METADATA).
   *
   * @return return type of block
   */
  public DataBlock.Type getType() {
    return _type;
  }

  /**
   * Return whether a transferable block is at the end of a stream.
   *
   * <p>End of stream is different from data block with 0-rows. which can indicate that one partition of the execution
   * returns no rows. but that doesn't mean the rest of the partitions are also finished.
   * <p>When an exception is caught within a stream, no matter how many outstanding data is pending to be received,
   * it is considered end of stream because the exception should bubble up immediately.
   *
   * @return whether this block is the end of stream.
   */
  // TODO: Update the name to isTerminateBlock.
  public boolean isEndOfStreamBlock() {
    return isType(MetadataBlock.MetadataBlockType.ERROR) || isType(MetadataBlock.MetadataBlockType.EOS);
  }

  /**
   * @return true when the block is a real end of stream block instead of error block.
   */
  public boolean isSuccessfulEndOfStreamBlock() {
    return isType(MetadataBlock.MetadataBlockType.EOS);
  }

  public boolean isDataBlock() {
    return _type != DataBlock.Type.METADATA;
  }

  /**
   * Return whether a transferable block contains exception.
   *
   * @return true if contains exception.
   */
  public boolean isErrorBlock() {
    return isType(MetadataBlock.MetadataBlockType.ERROR);
  }

  private boolean isType(MetadataBlock.MetadataBlockType type) {
    if (_type != DataBlock.Type.METADATA) {
      return false;
    }

    MetadataBlock metadata = (MetadataBlock) _dataBlock;
    return metadata.getType() == type;
  }

  @Override
  public String toString() {
    String blockType = isErrorBlock() ? "error" : isSuccessfulEndOfStreamBlock() ? "eos" : "data";
    return "TransferableBlock{blockType=" + blockType + ", _numRows=" + _numRows + '}';
  }
}
