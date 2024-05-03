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
package org.apache.pinot.common.datablock;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A block type to indicate some metadata about the current processing state.
 * For the different types of metadata blocks see {@link MetadataBlockType}.
 */
public class MetadataBlock extends BaseDataBlock {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataBlock.class);
  @VisibleForTesting
  static final int VERSION = 2;
  @Nullable
  private List<ByteBuffer> _statsByStage;

  private MetadataBlock() {
    this(Collections.emptyList());
  }

  public static MetadataBlock newEos() {
    return new MetadataBlock();
  }

  public static MetadataBlock newError(Map<Integer, String> exceptions) {
    MetadataBlock errorBlock = new MetadataBlock();
    for (Map.Entry<Integer, String> exception : exceptions.entrySet()) {
      errorBlock.addException(exception.getKey(), exception.getValue());
    }
    return errorBlock;
  }

  public MetadataBlock(List<ByteBuffer> statsByStage) {
    super(0, null, new String[0], new byte[0], new byte[0]);
    _statsByStage = statsByStage;
  }

  MetadataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
  }

  @Override
  protected void serializeMetadata(DataOutputStream output)
      throws IOException {
    if (_statsByStage == null) {
      output.writeInt(0);
      return;
    }
    int size = _statsByStage.size();
    output.writeInt(size);
    if (size > 0) {
      byte[] bytes = new byte[4096];
      for (ByteBuffer stat : _statsByStage) {
        if (stat == null) {
          output.writeBoolean(false);
        } else {
          output.writeBoolean(true);
          output.writeInt(stat.remaining());
          ByteBuffer duplicate = stat.duplicate();
          while (duplicate.hasRemaining()) {
            int length = Math.min(duplicate.remaining(), bytes.length);
            duplicate.get(bytes, 0, length);
            output.write(bytes, 0, length);
          }
        }
      }
    }
  }

  public static MetadataBlock deserialize(ByteBuffer byteBuffer, int version)
      throws IOException {
    switch (version) {
      case 1:
      case 2:
        return new MetadataBlock(byteBuffer);
      default:
        throw new IOException("Unsupported metadata block version: " + version);
    }
  }

  @Override
  protected void deserializeMetadata(ByteBuffer buffer)
      throws IOException {
    try {
      int statsSize = buffer.getInt();

      List<ByteBuffer> stats = new ArrayList<>(statsSize);

      for (int i = 0; i < statsSize; i++) {
        if (buffer.get() != 0) {
          int length = buffer.getInt();
          buffer.limit(buffer.position() + length);
          stats.add(buffer.slice());
          buffer.position(buffer.limit());
          buffer.limit(buffer.capacity());
        } else {
          stats.add(null);
        }
      }
      _statsByStage = stats;
    } catch (BufferUnderflowException e) {
      LOGGER.info("Failed to read stats from metadata block. Considering it empty", e);;
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to read stats from metadata block. Considering it empty", e);;
    }
  }

  public MetadataBlockType getType() {
    return _errCodeToExceptionMap.isEmpty() ? MetadataBlockType.EOS : MetadataBlockType.ERROR;
  }

  /**
   * Returns the list of serialized stats.
   * <p>
   * The returned list may contain nulls, which would mean that no stats were available for that stage.
   */
  @Nullable
  public List<ByteBuffer> getStatsByStage() {
    return _statsByStage;
  }

  @Override
  public int getDataBlockVersionType() {
    return VERSION + (Type.METADATA.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    throw new UnsupportedOperationException("Metadata block uses JSON encoding for field access");
  }

  @Override
  protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
    throw new UnsupportedOperationException("Metadata block uses JSON encoding for field access");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataBlock)) {
      return false;
    }
    MetadataBlock that = (MetadataBlock) o;
    return Objects.equals(_statsByStage, that._statsByStage)
        && _errCodeToExceptionMap.equals(that._errCodeToExceptionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_statsByStage, _errCodeToExceptionMap);
  }

  public enum MetadataBlockType {
    /**
     * Indicates that this block is the final block to be sent
     * (End Of Stream) as part of an operator chain computation.
     */
    EOS,

    /**
     * An {@code ERROR} metadata block indicates that there was
     * some error during computation. To retrieve the error that
     * occurred, use {@link MetadataBlock#getExceptions()}
     */
    ERROR
  }
}
