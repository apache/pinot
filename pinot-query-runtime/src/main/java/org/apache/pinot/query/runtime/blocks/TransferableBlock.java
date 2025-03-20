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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/**
 * A {@code TransferableBlock} is a wrapper around {@link DataBlock} for transferring data using
 * {@link org.apache.pinot.common.proto.Mailbox}.
 */
@SuppressWarnings("rawtypes")
public class TransferableBlock implements Block {
  private final DataBlock.Type _type;
  @Nullable
  private final DataSchema _dataSchema;
  private final int _numRows;

  private List<Object[]> _container;
  private DataBlock _dataBlock;
  private Map<Integer, String> _errCodeToExceptionMap;
  @Nullable
  private final MultiStageQueryStats _queryStats;

  @Nullable
  private AggregationFunction[] _aggFunctions;

  public TransferableBlock(List<Object[]> container, DataSchema dataSchema, DataBlock.Type type) {
    this(container, dataSchema, type, null);
  }

  public TransferableBlock(List<Object[]> container, DataSchema dataSchema, DataBlock.Type type,
      @Nullable AggregationFunction[] aggFunctions) {
    _container = container;
    _dataSchema = dataSchema;
    Preconditions.checkArgument(type == DataBlock.Type.ROW || type == DataBlock.Type.COLUMNAR,
        "Container cannot be used to construct block of type: %s", type);
    _type = type;
    _aggFunctions = aggFunctions;
    _numRows = _container.size();
    // NOTE: Use assert to avoid breaking production code.
    assert _numRows > 0 : "Container should not be empty";
    _errCodeToExceptionMap = new HashMap<>();
    _queryStats = null;
  }

  public TransferableBlock(DataBlock dataBlock) {
    _dataBlock = dataBlock;
    _dataSchema = dataBlock.getDataSchema();
    _type = dataBlock instanceof ColumnarDataBlock ? DataBlock.Type.COLUMNAR
        : dataBlock instanceof RowDataBlock ? DataBlock.Type.ROW : DataBlock.Type.METADATA;
    _numRows = _dataBlock.getNumberOfRows();
    _errCodeToExceptionMap = null;
    _queryStats = null;
  }

  public TransferableBlock(MultiStageQueryStats stats) {
    _queryStats = stats;
    _type = DataBlock.Type.METADATA;
    _numRows = 0;
    _dataSchema = null;
    _errCodeToExceptionMap = null;
  }

  public List<DataBuffer> getSerializedStatsByStage() {
    if (isSuccessfulEndOfStreamBlock()) {
      List<DataBuffer> statsByStage;
      if (_dataBlock instanceof MetadataBlock) {
        statsByStage = _dataBlock.getStatsByStage();
        if (statsByStage == null) {
          return new ArrayList<>();
        }
      } else {
        Preconditions.checkArgument(_queryStats != null, "QueryStats is null for a successful EOS block");
        try {
          statsByStage = _queryStats.serialize();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      return statsByStage;
    }
    return new ArrayList<>();
  }

  @Nullable
  public MultiStageQueryStats getQueryStats() {
    return _queryStats;
  }

  public int getNumRows() {
    return _numRows;
  }

  @Nullable
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Returns whether the container is already constructed.
   */
  public boolean isContainerConstructed() {
    return _container != null;
  }

  /**
   * Retrieve the extracted {@link TransferableBlock#_container} of the transferable block.
   * If not already constructed. It will use {@link DataBlockUtils} to extract the row/columnar data from the
   * binary-packed format.
   *
   * TODO: This method should never been called by operators, as it allocates a lot for no reason.
   *   Instead, an iterable should be returned.
   *   That iterable can materialize rows one by one, without allocating all of them at once.
   *   In fact transformations and filters could be implemented in zero allocate fashion by having a special type of
   *   block that wraps the child block and decorates it with a transformation/predicate.
   *   By doing so only operators that actually require to keep multi-stage results in memory will allocate memory.
   *   PS: the term _allocate memory_ here means _keep alive an amount of memory proportional to the number of rows_.
   *
   * @return data container.
   */
  public List<Object[]> getContainer() {
    if (_container == null) {
      switch (_type) {
        case ROW:
          _container = DataBlockExtractUtils.extractRows(_dataBlock);
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
            _dataBlock = DataBlockBuilder.buildFromRows(_container, _dataSchema, _aggFunctions);
            break;
          case COLUMNAR:
            _dataBlock = DataBlockBuilder.buildFromColumns(_container, _dataSchema, _aggFunctions);
            break;
          case METADATA:
            _dataBlock = new MetadataBlock(getSerializedStatsByStage());
            break;
          default:
            throw new UnsupportedOperationException("Unable to construct block with type: " + _type);
        }
        if (_errCodeToExceptionMap != null) {
          _dataBlock.getExceptions().putAll(_errCodeToExceptionMap);
          _errCodeToExceptionMap = null;
        }
      } catch (Exception e) {
        throw new RuntimeException("Unable to create DataBlock", e);
      }
    }
    return _dataBlock;
  }

  public Map<Integer, String> getExceptions() {
    return _dataBlock != null ? _dataBlock.getExceptions() : _errCodeToExceptionMap;
  }

  /**
   * Return the type of block (one of ROW, COLUMNAR, or METADATA).
   *
   * @return return type of block
   */
  public DataBlock.Type getType() {
    return _type;
  }

  @Nullable
  public AggregationFunction[] getAggFunctions() {
    return _aggFunctions;
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

    if (_queryStats != null) {
      return MetadataBlock.MetadataBlockType.EOS == type;
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
