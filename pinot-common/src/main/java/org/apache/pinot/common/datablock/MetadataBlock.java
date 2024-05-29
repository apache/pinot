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
import java.io.DataOutput;
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
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotOutputStream;
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
  private List<DataBuffer> _statsByStage;

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

  public MetadataBlock(List<DataBuffer> statsByStage) {
    super(0, null, new String[0], new byte[0], new byte[0]);
    _statsByStage = statsByStage;
  }

  MetadataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
  }

  @Override
  protected void serializeMetadata(DataOutput output)
      throws IOException {
    if (_statsByStage == null) {
      output.writeInt(0);
      return;
    }
    int size = _statsByStage.size();
    output.writeInt(size);
    if (size > 0) {
      byte[] bytes = new byte[4096];
      for (DataBuffer stat : _statsByStage) {
        if (stat == null) {
          output.writeBoolean(false);
        } else {
          output.writeBoolean(true);
          if (stat.size() > Integer.MAX_VALUE) {
            throw new IOException("Stat size is too large to serialize");
          }
          output.writeInt((int) stat.size());
          int copied = 0;
          while (copied < stat.size()) {
            int length = (int) Math.min(stat.size() - copied, bytes.length);
            stat.copyTo(copied, bytes, 0, length);
            output.write(bytes, 0, length);
            copied += length;
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

      List<DataBuffer> stats = new ArrayList<>(statsSize);

      for (int i = 0; i < statsSize; i++) {
        if (buffer.get() != 0) {
          int length = buffer.getInt();
          buffer.limit(buffer.position() + length);
          stats.add(PinotByteBuffer.wrap(buffer.slice()));
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
  @Override
  public List<DataBuffer> getStatsByStage() {
    return _statsByStage;
  }

  @Override
  public int getDataBlockVersionType() {
    return VERSION + (Type.METADATA.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
  }

  @Override
  public Type getDataBlockType() {
    return Type.METADATA;
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    throw new UnsupportedOperationException("Metadata block uses JSON encoding for field access");
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
