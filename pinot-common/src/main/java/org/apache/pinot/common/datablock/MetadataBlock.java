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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * A block type to indicate some metadata about the current processing state.
 * For the different types of metadata blocks see {@link MetadataBlockType}.
 */
public class MetadataBlock extends BaseDataBlock {

  private static final ObjectMapper JSON = new ObjectMapper();

  @VisibleForTesting
  static final int VERSION = 1;

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
    ERROR,

    /**
     * A {@code NOOP} metadata block can be sent at any point to
     * and should be ignored by downstream - it is often used to
     * indicate that the operator chain either has nothing to process
     * or has processed data but is not yet ready to emit a result
     * block.
     */
    NOOP;

    MetadataBlockType() {
    }
  }

  /**
   * Used to serialize the contents of the metadata block conveniently and in
   * a backwards compatible way. Use JSON because the performance of metadata block
   * SerDe should not be a bottleneck.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @VisibleForTesting
  static class Contents {

    private String _type;

    @JsonCreator
    public Contents(@JsonProperty("type") String type) {
      _type = type;
    }

    @JsonCreator
    public Contents() {
      _type = null;
    }

    public String getType() {
      return _type;
    }

    public void setType(String type) {
      _type = type;
    }
  }

  private final Contents _contents;

  public MetadataBlock(MetadataBlockType type) {
    super(0, null, new String[0], new byte[]{0}, toContents(new Contents(type.name())));
    _contents = new Contents(type.name());
  }

  private static byte[] toContents(Contents type) {
    try {
      return JSON.writeValueAsBytes(type);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public MetadataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
    if (_variableSizeDataBytes != null) {
      _contents = JSON.readValue(_variableSizeDataBytes, Contents.class);
    } else {
      _contents = new Contents();
    }
  }

  public MetadataBlockType getType() {
    String type = _contents.getType();

    // if type is null, then we're reading a legacy block where we didn't encode any
    // data. assume that it is an EOS block if there's no exceptions and an ERROR block
    // otherwise
    return type == null
        ? (getExceptions().isEmpty() ? MetadataBlockType.EOS : MetadataBlockType.ERROR)
        : MetadataBlockType.valueOf(type);
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
  public MetadataBlock toMetadataOnlyDataTable() {
    return this;
  }

  @Override
  public MetadataBlock toDataOnlyDataTable() {
    throw new UnsupportedOperationException();
  }
}
