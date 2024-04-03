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
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A block type to indicate some metadata about the current processing state.
 * For the different types of metadata blocks see {@link MetadataBlockType}.
 */
public class MetadataBlock extends BaseDataBlock {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataBlock.class);
  @VisibleForTesting
  static final int VERSION = 1;

  private final MetadataBlockType _type;

  public MetadataBlock(MetadataBlockType type) {
    this(type, Collections.emptyList());
  }

  public MetadataBlock(MetadataBlockType type, List<ByteBuffer> stats) {
    super(0, null, new String[0], new byte[]{(byte) (type.ordinal() & 0xFF)}, serializeStats(stats));
    _type = type;
  }

  private static byte[] serializeStats(List<ByteBuffer> stats) {
    try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream(4096);
        DataOutputStream output = new DataOutputStream(baos)
    ) {
      int size = stats.size();
      output.writeInt(size);
      if (size > 0) {
        byte[] bytes = new byte[4096];
        for (ByteBuffer stat : stats) {
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
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public MetadataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
    if (_fixedSizeDataBytes == null) {
       if (_errCodeToExceptionMap.isEmpty()) {
         _type = MetadataBlockType.EOS;
       } else {
         _type = MetadataBlockType.ERROR;
       }
    } else {
      _type = MetadataBlockType.values()[_fixedSizeDataBytes[0]];
    }
  }

  public MetadataBlockType getType() {
    return _type;
  }

  /**
   * Returns the list of serialized stats.
   *
   * The returned list may contain nulls, which would mean that no stats were available for that stage.
   */
  @Nullable
  public List<ByteBuffer> getStatsByStage() {
    if (_variableSizeData == null || _variableSizeData.capacity() == 0) {
      return null;
    }
    try {
      _variableSizeData.clear();
      int statsSize = _variableSizeData.getInt();

      List<ByteBuffer> stats = new ArrayList<>(statsSize);

      for (int i = 0; i < statsSize; i++) {
        if (_variableSizeData.get() != 0) {
          int length = _variableSizeData.getInt();
          _variableSizeData.limit(_variableSizeData.position() + length);
          stats.add(_variableSizeData.slice());
          _variableSizeData.position(_variableSizeData.limit());
          _variableSizeData.limit(_variableSizeData.capacity());
        } else {
          stats.add(null);
        }
      }
      return stats;
    } catch (BufferUnderflowException e) {
      LOGGER.info("Failed to read stats from metadata block. Considering it empty", e);
      return null;
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to read stats from metadata block. Considering it empty", e);
      return null;
    }
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
  public MetadataBlock toDataOnlyDataTable() {
    throw new UnsupportedOperationException();
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
